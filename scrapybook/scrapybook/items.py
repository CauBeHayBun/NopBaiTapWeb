import scrapy

class MyscraptbookCrawlerItem(scrapy.Item):
    coursename = scrapy.Field()  # Tên khóa học
    tacgia = scrapy.Field()       # Tác giả
    tietkiem = scrapy.Field()     # Tiết kiệm
    giahientai = scrapy.Field()   # Giá hiện tại
    giathitruong = scrapy.Field()           # Có
    tinhtrang = scrapy.Field()    # Tình trạng
    dichgia = scrapy.Field()      # Địch giá
    nhaxuatban = scrapy.Field()    # Nhà xuất bản
    sotrang = scrapy.Field()      # Số trang
    trongluong = scrapy.Field()   # Trọng lượng
    noidung_brand = scrapy.Field() # Nội dung brand

