import os
import mysql.connector
from dotenv import load_dotenv
import requests
import json
import html

# Ortam değişkenlerini yükle
load_dotenv()

class OpencartToWooCommerce:
    def __init__(self):
        # Opencart veritabanı bağlantısı
        self.opencart_db = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        
        # WooCommerce API ayarları
        self.wc_url = os.getenv('WC_STORE_URL') + '/wp-json/wc/v3'
        self.wc_auth = (
            os.getenv('WC_CONSUMER_KEY'),
            os.getenv('WC_CONSUMER_SECRET')
        )
        
        # Kategori eşleştirme cache
        self.category_map = {}
    
    def get_or_create_wc_category(self, category_name, parent_id=0):
        """WooCommerce'te kategori oluşturur veya var olanı getirir"""
        if category_name in self.category_map:
            return self.category_map[category_name]
            
        # Kategori var mı kontrol et
        response = requests.get(
            f"{self.wc_url}/products/categories",
            auth=self.wc_auth,
            params={'search': category_name}
        )
        
        if response.status_code == 200:
            categories = response.json()
            for cat in categories:
                if cat['name'].lower() == category_name.lower():
                    self.category_map[category_name] = cat['id']
                    return cat['id']
        
        # Yeni kategori oluştur
        data = {
            'name': category_name,
            'parent': parent_id
        }
        
        response = requests.post(
            f"{self.wc_url}/products/categories",
            auth=self.wc_auth,
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 201:
            cat_id = response.json()['id']
            self.category_map[category_name] = cat_id
            return cat_id
        else:
            print(f"Kategori oluşturulamadı: {category_name}")
            return 0
    
    def get_opencart_products(self, category_id=66):
        """Opencart'tan ürünleri, kategorilerini ve varyasyonlarını çeker
        Args:
            category_id (int): Sadece bu kategori ID'sine ait ürünleri getir
        """
        cursor = self.opencart_db.cursor(dictionary=True)
        
        # Ürün bilgilerini al (sadece belirtilen kategoriye ait olanlar)
        query = f"""
        SELECT p.product_id, pd.name, p.model, p.price, pd.description, p.image
        FROM oc_product p
        JOIN oc_product_description pd ON p.product_id = pd.product_id
        JOIN oc_product_to_category pc ON p.product_id = pc.product_id
        WHERE pd.language_id = 1
        AND pc.category_id = {category_id}
        """
        cursor.execute(query)
        products = cursor.fetchall()
        
        # Ürün resimlerini ve kategorilerini al
        for product in products:
            # Ana resim
            main_image = f"{os.getenv('DB_HOST_DOMAIN')}/image/{product['image']}" if product['image'] else None
            product['images'] = [main_image] if main_image else []
            
            # Diğer resimler
            cursor.execute(f"""
            SELECT image FROM oc_product_image 
            WHERE product_id = {product['product_id']}
            """)
            images = cursor.fetchall()
            product['images'].extend([f"{os.getenv('DB_HOST_DOMAIN')}/image/{img['image']}" for img in images if img['image']])
            
            # Varyasyonları detaylı şekilde al
            cursor.execute(f"""
            SELECT 
                od.name as option_name,
                ovd.name as option_value,
                pov.price,
                pov.price_prefix,
                pov.quantity as stock_quantity
            FROM oc_product_option_value pov
            JOIN oc_option_value_description ovd ON pov.option_value_id = ovd.option_value_id
            JOIN oc_product_option po ON pov.product_option_id = po.product_option_id
            JOIN oc_option_description od ON po.option_id = od.option_id
            WHERE po.product_id = {product['product_id']} 
            AND ovd.language_id = 1 
            AND od.language_id = 1
            """)
            variations = cursor.fetchall()
            
            product['options_data'] = self.process_variations(variations, float(product['price']))
            
            # Kategorileri al (hepsini almaya devam ediyoruz)
            cursor.execute(f"""
            SELECT cd.name FROM oc_product_to_category pc
            JOIN oc_category_description cd ON pc.category_id = cd.category_id
            WHERE pc.product_id = {product['product_id']} AND cd.language_id = 1
            """)
            product['categories'] = [cat['name'] for cat in cursor.fetchall()]
        
        cursor.close()
        return products
    
    def process_variations(self, variations, product_price):
        """Varyasyonları WooCommerce formatına dönüştürür"""
        attributes = {}
        variation_list = []

        for var in variations:
            # Nitelikleri oluştur
            if var['option_name'] not in attributes:
                attributes[var['option_name']] = {
                    'name': var['option_name'],
                    'options': [],
                    'visible': True,
                    'variation': True
                }
            attributes[var['option_name']]['options'].append(var['option_value'])

            # Varyasyon fiyatını hesapla
            var_price = float(var.get('price', 0))
            
            # Price_prefix'e göre fiyatı hesapla
            if var['price_prefix'] == '+':
                variation_price = product_price + var_price
            elif var['price_prefix'] == '-':
                variation_price = product_price - var_price
            else:
                # Prefix yoksa veya farklı bir değerse, varyasyon fiyatını kullan
                # Eğer varyasyon fiyatı 0 ise, ürün fiyatını kullan
                variation_price = var_price if var_price > 0 else product_price

            # Varyasyon verisi
            variation = {
                'attributes': [{
                    'name': var['option_name'],
                    'option': var['option_value']
                }],
                'regular_price': str(round(variation_price, 2)),
                'stock_quantity': var.get('stock_quantity', 0),
                'manage_stock': True
            }
            variation_list.append(variation)

        return {
            'attributes': list(attributes.values()),
            'variations': variation_list
        }
        
    def create_woocommerce_product(self, product):
        """WooCommerce'e varyasyonlu ürün ekler"""
        # Ana ürün verisi
        data = {
            'name': product['name'],
            'type': 'variable',
            'description': html.unescape(product['description']),
            'short_description': '',
            'categories': [{'id': self.get_or_create_wc_category(cat)} for cat in product['categories']],
            'images': [{'src': img} for img in product['images']],
            'attributes': product['options_data']['attributes'],
            'default_attributes': [],
            'meta_data': [
                {'key': 'opencart_id', 'value': product['product_id']}
            ]
        }
        
        # Ana ürünü oluştur
        response = requests.post(
            f"{self.wc_url}/products",
            auth=self.wc_auth,
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 201:
            product_id = response.json()['id']
            print(f"✅ Ana ürün oluşturuldu: {product['name']}")
            
            # Varyasyonları ekle
            for variation in product['options_data']['variations']:
                var_response = requests.post(
                    f"{self.wc_url}/products/{product_id}/variations",
                    auth=self.wc_auth,
                    json=variation,
                    headers={'Content-Type': 'application/json'}
                )
                if var_response.status_code == 201:
                    print(f"  ↳ Varyasyon eklendi: {variation['attributes'][0]['option']} ({variation['regular_price']}₺)")
                else:
                    print(f"  ↳ ❌ Varyasyon hatası: {var_response.text}")
        else:
            print(f"❌ Ürün oluşturma hatası: {response.text}")
    
    def transfer_products(self, category_id=66):
        """Belirtilen kategori ID'sine ait ürünleri aktarır
        Args:
            category_id (int): Aktarılacak ürünlerin kategori ID'si (varsayılan: 66)
        """
        products = self.get_opencart_products(category_id)
        total_products = len(products)
        
        print(f"Kategori ID: {category_id} için ürünler aktarılıyor (Toplam: {total_products} ürün)")
        
        for i, product in enumerate(products, 1):
            self.create_woocommerce_product(product)
            print(f"[{i}/{total_products}] {product['name']} ürünü ve varyasyonları aktarıldı")
        
        print("Aktarım tamamlandı!")

if __name__ == '__main__':
    transfer = OpencartToWooCommerce()
    
    # Sadece kategori ID'si 66 olan ürünleri aktar
    transfer.transfer_products(category_id=66)