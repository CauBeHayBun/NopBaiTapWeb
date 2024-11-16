IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'authors' AND xtype = 'U')
BEGIN
    CREATE TABLE authors (
        authors_id INT IDENTITY(1,1) PRIMARY KEY,  -- Tạo cột authors_id với kiểu INT và auto increment
        tacgia NVARCHAR(255)
    );
END;

-- Tạo bảng publishers
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'publishers' AND xtype = 'U')
BEGIN
    CREATE TABLE publishers (
        publisher_id INT IDENTITY(1,1) PRIMARY KEY,  -- Tạo cột publisher_id với kiểu INT và auto increment
        nhaxuatban NVARCHAR(255)
    );
END;

-- Tạo bảng bookdetails
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'bookdetails' AND xtype = 'U')
BEGIN
    CREATE TABLE bookdetails (
        bookdetails_id INT IDENTITY(1,1) PRIMARY KEY,  -- Tạo cột bookdetails_id với kiểu INT và auto increment
        sotrang INT,
        trongluong INT,
        noidung_brand TEXT,
        dichgia NVARCHAR(255)
    );
END;

-- Tạo bảng pricing
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'pricing' AND xtype = 'U')
BEGIN
    CREATE TABLE pricing (
        pricing_id INT IDENTITY(1,1) PRIMARY KEY,  -- Tạo cột pricing_id với kiểu INT và auto increment
        giahientai INT,
        giathitruong INT,
        tietkiem INT
    );
END;

-- Tạo bảng books
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'books' AND xtype = 'U')
BEGIN
    CREATE TABLE books (
        book_id INT IDENTITY(1,1) PRIMARY KEY,  -- Tạo cột book_id với kiểu INT và auto increment
        coursename NVARCHAR(255),
        publisher_id INT,
        pricing_id INT,
        bookdetails_id INT,
        authors_id INT,
        CONSTRAINT FK_books_publishers FOREIGN KEY (publisher_id) REFERENCES publishers(publisher_id),
        CONSTRAINT FK_books_pricing FOREIGN KEY (pricing_id) REFERENCES pricing(pricing_id),
        CONSTRAINT FK_books_bookdetails FOREIGN KEY (bookdetails_id) REFERENCES bookdetails(bookdetails_id),
        CONSTRAINT FK_books_authors FOREIGN KEY (authors_id) REFERENCES authors(authors_id)
    );
END;

select * from [dbo].[Sheet1];