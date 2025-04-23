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
    
    def get_opencart_products(self):
        """Opencart'tan ürünleri, kategorilerini ve seçeneklerini çeker"""
        cursor = self.opencart_db.cursor(dictionary=True)
        
        # Ürün bilgilerini al
        query = """
        SELECT p.product_id, pd.name, p.model, p.price, pd.description, p.image
        FROM oc_product p
        JOIN oc_product_description pd ON p.product_id = pd.product_id
        WHERE pd.language_id = 1
        LIMIT 2
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
            
            # Seçenekleri detaylı şekilde al
            cursor.execute(f"""
            SELECT 
                od.name as option_name,
                ovd.name as option_value,
                o.price,
                o.price_prefix
            FROM oc_product_option_value o
            JOIN oc_option_value_description ovd ON o.option_value_id = ovd.option_value_id
            JOIN oc_product_option po ON o.product_option_id = po.product_option_id
            JOIN oc_option_description od ON po.option_id = od.option_id
            WHERE po.product_id = {product['product_id']} AND ovd.language_id = 1 AND od.language_id = 1
            """)
            options = cursor.fetchall()
            product['attributes'] = self.process_options(options)
            
            # Kategoriler
            cursor.execute(f"""
            SELECT cd.name 
            FROM oc_product_to_category pc
            JOIN oc_category_description cd ON pc.category_id = cd.category_id
            WHERE pc.product_id = {product['product_id']} AND cd.language_id = 1
            """)
            categories = cursor.fetchall()
            product['categories'] = [cat['name'] for cat in categories]
        
        cursor.close()
        return products
    
    def process_options(self, options):
        """Seçenekleri WooCommerce attribute formatına dönüştürür"""
        attributes = {}
        for option in options:
            if option['option_name'] not in attributes:
                attributes[option['option_name']] = {
                    'name': option['option_name'],
                    'options': [],
                    'visible': True,
                    'variation': True
                }
            attributes[option['option_name']]['options'].append(option['option_value'])
        return list(attributes.values())
    
    def create_woocommerce_product(self, product):
        """WooCommerce'e ürün, nitelikleri ve kategorilerini ekler"""
        # Önce kategorileri oluştur
        wc_categories = []
        for cat_name in product.get('categories', []):
            cat_id = self.get_or_create_wc_category(cat_name)
            if cat_id:
                wc_categories.append({'id': cat_id})
        
        # HTML içeriğini decode et
        decoded_description = html.unescape(product['description'])
        
        data = {
            'name': product['name'],
            'type': 'simple',
            'regular_price': str(product['price']),
            'description': decoded_description,
            'sku': product['model'],
            'images': [{'src': img} for img in product['images']],
            'categories': wc_categories,
            'attributes': product.get('attributes', []),
            'meta_data': [
                {'key': 'opencart_id', 'value': product['product_id']}
            ]
        }
        
        # API isteği gönder
        response = requests.post(
            f"{self.wc_url}/products",
            auth=self.wc_auth,
            json=data,
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status_code == 201:
            print(f"Ürün başarıyla eklendi: {product['name']}")
        else:
            print(f"Hata oluştu: {response.text}")
    
    def transfer_products(self):
        """Tüm ürünleri ve kategorilerini aktarır"""
        products = self.get_opencart_products()
        for product in products[:2]:  # Sadece ilk 2 ürün
            self.create_woocommerce_product(product)
            print(f"{product['name']} ürünü aktarıldı (Toplam: {len(products)} ürün var)")
        print("İlk 2 ürün aktarımı tamamlandı!")

if __name__ == '__main__':
    transfer = OpencartToWooCommerce()
    transfer.transfer_products()
