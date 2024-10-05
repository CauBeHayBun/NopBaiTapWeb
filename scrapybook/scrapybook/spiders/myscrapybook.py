import scrapy
from scrapybook.items import MyscraptbookCrawlerItem
import re


class myscrapybook(scrapy.Spider):
    name = "myscrapybook"
    allowed_domains = ["bookbuy.vn"]
    start_urls = [f'https://bookbuy.vn/sach/sach-kinh-te?Page={i}' for i in range(1, 60)]

    def __init__(self, *args, **kwargs):
        super(myscrapybook, self).__init__(*args, **kwargs)
        self.data = []

    def parse(self, response):
        # Lấy liên kết chi tiết của từng sách trên trang
        book_links = response.xpath('//*[@id="bb-body"]/div/div/div[4]/div[2]/div/div/div[1]/a/@href').getall()
        for link in book_links:
            yield response.follow(link, callback=self.parse_book_details)

    def parse_book_details(self, response):
        item = MyscraptbookCrawlerItem()
        item['coursename'] = response.xpath('normalize-space(//*[@id="bb-body"]/div[1]/div[1]/div[2]/div/div[2]/div/div[1]/h1/text())').get()
        item['tacgia'] = response.xpath('normalize-space(//*[@id="bb-body"]/div[1]/div[1]/div[2]/div/div[2]/div/div[1]/div[1]/div[1]/a/h2/text())').get()
        item['tietkiem'] = response.xpath('normalize-space(//*[@id="bb-body"]/div[1]/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div[1]/p[2]/text())').get()
        # Kiểm tra xem có dữ liệu hay không
        if item['tietkiem']:
    # Lấy phần chuỗi trước dấu mở ngoặc (tức là chỉ giữ phần số tiền)
            item['tietkiem'] = item['tietkiem'].split('(')[0].strip()
    
    # Loại bỏ các ký tự không phải số (ví dụ: "đ", dấu phẩy)
            item['tietkiem'] = re.sub(r'[^\d]', '', item['tietkiem'])
    
    # Chuyển chuỗi thành số nguyên
            item['tietkiem'] = int(item['tietkiem'])
        item['giahientai'] = response.xpath('normalize-space(//*[@id="bb-body"]/div[1]/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/p[1]/text())').get()
        item['giathitruong'] = response.xpath('normalize-space(//*[@id="bb-body"]/div[1]/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/p[2]/text())').get()
        item['tinhtrang'] = response.xpath('normalize-space(//*[@id="bb-body"]/div[1]/div[1]/div[2]/div/div[2]/div/div[1]/div[3]/div[4]/p[2]/text())').get()
        item['dichgia'] = response.xpath('normalize-space(//*[@id="bb-body"]/div[1]/div[1]/div[2]/div/div[2]/div/div[1]/div[1]/div[3]/a/h2)').get()
        item['nhaxuatban'] = response.xpath('normalize-space(//li[contains(text(), "Nhà xuất bản")]/a/text())').get()
        item['sotrang'] = response.xpath('normalize-space(//li[contains(., "Số trang")]/span/text())').get()
        item['trongluong'] = response.xpath('normalize-space(//li[contains(., "Trọng lượng")]/span/text())').get()
        noidung_brand = response.xpath('//div[@class="des-des"]//text()').getall()
        item['noidung_brand'] = ' '.join([text.strip() for text in noidung_brand]).replace('\n', '').replace('\r', '').strip()

        yield item