use dataset;

LOAD DATA INFILE 'C:/Users/hi/Documents/Data Analis/data_output.csv'
INTO TABLE transaksi
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

--overview--

CREATE VIEW monthly_revenue AS
SELECT DATE_FORMAT(InvoiceDate, '%Y-%m') AS Month, SUM(Revenue) AS Revenue
FROM transaksi
GROUP BY Month;

create view top_product as
SELECT `Description`, SUM(Revenue) AS TotalRevenue
FROM transaksi
GROUP BY `Description`
ORDER BY TotalRevenue DESC
LIMIT 5;

CREATE VIEW Top_5_Countries_and_Others AS
(
    SELECT
        Country,
        SUM(Quantity * UnitPrice) AS Revenue,
        'Top 5' AS Category
    FROM
        transaksi
    GROUP BY
        Country
    ORDER BY
        Revenue DESC
    LIMIT 5
)
UNION ALL
(
    SELECT
        'Other' AS Country,
        SUM(T1.Revenue) AS Revenue,
        'Others' AS Category
    FROM
        (
            SELECT
                SUM(Quantity * UnitPrice) AS Revenue
            FROM
                transaksi
            GROUP BY
                Country
            ORDER BY
                Revenue DESC
            LIMIT 999999999999
            OFFSET 5
        ) AS T1
);

create view banyak_transaksi as
select date_format(invoicedate, '%Y-%m') as invoice_month,
	   count(distinct customerID) as JumlahCustomer
from transaksi
group by
	invoice_month


--sales & product
CREATE TABLE numbers (
    n INT PRIMARY KEY
);

-- Masukkan angka dari 0 sampai 9
INSERT INTO numbers (n) VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);

select * from numbers
DELETE FROM numbers WHERE n > 374;

-- Tambahkan baris untuk mencapai 1000 angka
INSERT INTO numbers (n) SELECT n + 10 FROM numbers; 
INSERT INTO numbers (n) SELECT n + 20 FROM numbers; 
INSERT INTO numbers (n) SELECT n + 40 FROM numbers;
INSERT INTO numbers (n) SELECT n + 80 FROM numbers;
INSERT INTO numbers (n) SELECT n + 160 FROM numbers;
INSERT INTO numbers (n) SELECT n + 320 FROM numbers;
INSERT INTO numbers (n) SELECT n + 640 FROM numbers;
    
CREATE VIEW view_daily_revenue AS
SELECT
    dates.full_date AS transaction_date,
    COALESCE(SUM(t.Revenue), 0) AS daily_revenue
FROM
    (
        SELECT
            DATE_ADD('2010-12-01', INTERVAL n DAY) AS full_date
        FROM
            numbers
        WHERE
            DATE_ADD('2010-12-01', INTERVAL n DAY) <= '2011-12-09'
    ) AS dates
LEFT JOIN
    transaksi AS t ON dates.full_date = DATE(t.InvoiceDate)
GROUP BY
    dates.full_date
ORDER BY
    dates.full_date;	
    
create view korelasi_quantity_unitprice as
SELECT
    UnitPrice,
    Quantity
FROM
    transaksi
WHERE
    Quantity < 50 AND UnitPrice < 50;
    
create view distribus_kuantitas_pembelian as
SELECT
    -- Rumus baru untuk memastikan bin dimulai dari 1
    (FLOOR((Quantity - 1) / 5) * 5) + 1 AS quantity_bin_start,
    COUNT(*) AS frequency
FROM
    transaksi
WHERE
    Quantity >= 1 AND Quantity < 100 -- Pastikan kuantitas lebih dari 0
GROUP BY
    quantity_bin_start
ORDER BY
    quantity_bin_start;
    
create view distribus_UnitPrice_pembelian3 as
SELECT
    -- Rumus baru untuk memastikan bin dimulai dari 1
    (FLOOR((unitprice - 1) / 5) * 5) + 1 AS UnitPrice_bin_start,
    COUNT(*) AS frequency
FROM
    transaksi
WHERE
    unitprice >= 1 AND unitprice < 50 -- Pastikan kuantitas lebih dari 0
GROUP BY
    UnitPrice_bin_start
ORDER BY
    UnitPrice_bin_start

CREATE VIEW basket_size2 AS
SELECT
    calendar.day_date AS InvoiceDate,
    COALESCE(daily_baskets.average_basket_size, 0) AS average_basket_size
FROM
    (
        -- Bagian ini membuat deret tanggal dari tanggal terawal sampai terakhir
        -- Batas tanggal dihitung menggunakan subquery langsung (menggantikan variabel)
        SELECT
            DATE_ADD(
                (SELECT MIN(CAST(InvoiceDate AS DATE)) FROM transaksi),
                INTERVAL (t.rn - 1) DAY
            ) AS day_date
        FROM
            (
                SELECT
                    ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS rn
                FROM
                    information_schema.columns
                LIMIT 10000 -- Sesuaikan batas ini dengan rentang hari di data Anda
            ) AS t
        WHERE
            DATE_ADD(
                (SELECT MIN(CAST(InvoiceDate AS DATE)) FROM transaksi),
                INTERVAL (t.rn - 1) DAY
            ) <= (SELECT MAX(CAST(InvoiceDate AS DATE)) FROM transaksi)
    ) AS calendar
LEFT JOIN
    (
        -- Bagian ini menghitung rata-rata basket size per hari
        SELECT
            CAST(t_inner.InvoiceDate AS DATE) AS InvoiceDate,
            AVG(t_inner.BasketSize) AS average_basket_size
        FROM
            (
                SELECT
                    CAST(t.InvoiceDate AS DATE) AS InvoiceDate,
                    SUM(t.Quantity) AS BasketSize
                FROM
                    transaksi t -- Beri alias 't'
                GROUP BY
                    t.InvoiceNo,
                    CAST(t.InvoiceDate AS DATE) -- Kolom dikualifikasi dengan alias 't' untuk menghindari ambiguitas (Error 1052)
            ) AS t_inner
        GROUP BY
            InvoiceDate
    ) AS daily_baskets ON calendar.day_date = daily_baskets.InvoiceDate
ORDER BY
    calendar.day_date;
    
--customer & country

create view RFM as
WITH rfm_values AS (
    SELECT
        CustomerID,
        -- Recency: Menghitung selisih hari dari tanggal transaksi terakhir hingga tanggal snapshot.
        DATEDIFF((SELECT MAX(InvoiceDate) FROM transaksi) + INTERVAL 1 DAY, MAX(InvoiceDate)) AS Recency,
        -- Frequency: Menghitung jumlah transaksi unik per pelanggan.
        COUNT(DISTINCT InvoiceNo) AS Frequency,
        -- Monetary: Menghitung total pengeluaran per pelanggan.
        SUM(Quantity * UnitPrice) AS Monetary
    FROM
       transaksi
    GROUP BY
        CustomerID
),
rfm_scores AS (
    SELECT
        CustomerID,
        Recency,
        Frequency,
        Monetary,
        -- R_Score: NTILE(4) akan memberi skor 1-4, diurutkan terbalik.
        NTILE(4) OVER (ORDER BY Recency DESC) AS R_Score,
        -- F_Score: NTILE(4) akan memberi skor 1-4.
        NTILE(4) OVER (ORDER BY Frequency) AS F_Score,
        -- M_Score: NTILE(4) akan memberi skor 1-4.
        NTILE(4) OVER (ORDER BY Monetary) AS M_Score
    FROM
        rfm_values
)

SELECT
    CustomerID,
    Recency,
    Frequency,
    Monetary,
    CONCAT(R_Score, F_Score, M_Score) AS RFM_Score,
    CASE
        WHEN CONCAT(R_Score, F_Score, M_Score) IN ('444', '434', '443') THEN 'Champions'
        WHEN CONCAT(R_Score, F_Score, M_Score) IN ('421', '411') THEN 'New Customers'
        WHEN CONCAT(R_Score, F_Score, M_Score) IN ('334', '344', '343') THEN 'Loyal Customers'
        WHEN CONCAT(R_Score, F_Score, M_Score) IN ('111', '112') THEN 'Lost Customers'
        ELSE 'Others'
    END AS Segment
FROM
    rfm_scores;
    
SET @start_date = (SELECT MIN(InvoiceDate) FROM transaksi);
SET @end_date = (SELECT MAX(InvoiceDate) FROM transaksi);

CREATE VIEW pertumbuhan_customer AS
WITH
-- Definisi tanggal awal dan akhir menggunakan CTE
date_range AS (
    SELECT
        MIN(InvoiceDate) AS start_date,
        MAX(InvoiceDate) AS end_date
    FROM transaksi
),
-- Langkah 1: Hitung tanggal pendaftaran pelanggan pertama
first_purchases AS (
    SELECT
        CustomerID,
        DATE_FORMAT(MIN(InvoiceDate), '%Y-%m') AS registration_month
    FROM
        transaksi
    GROUP BY
        CustomerID
),
-- Langkah 2: Buat deret angka yang akan digunakan untuk kalender
numbers AS (
    SELECT
        ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS rn
    FROM
        information_schema.columns
    LIMIT 200 -- Sesuaikan batas ini
),
-- Langkah 3: Gunakan deret angka untuk membuat kalender bulan lengkap
calendar AS (
    SELECT
        DATE_FORMAT(DATE_ADD(dr.start_date, INTERVAL (rn - 1) MONTH), '%Y-%m') AS month_name
    FROM
        numbers, date_range AS dr
    WHERE
        DATE_ADD(dr.start_date, INTERVAL (rn - 1) MONTH) <= dr.end_date
)
-- Langkah 4: Gabungkan kalender dan data pelanggan untuk mendapatkan hasil akhir
SELECT
    c.month_name AS RegistrationMonth,
    COUNT(fp.CustomerID) AS NewCustomers
FROM
    calendar AS c
LEFT JOIN
    first_purchases AS fp ON c.month_name = fp.registration_month
GROUP BY
    c.month_name
ORDER BY
    c.month_name;

create view pelanggan_hilang as
WITH last_transactions AS (
    -- Langkah 1: Temukan tanggal transaksi terakhir untuk setiap pelanggan
    SELECT
        CustomerID,
        MAX(InvoiceDate) AS LastInvoiceDate
    FROM
        transaksi
    GROUP BY
        CustomerID
)

-- Langkah 2: Kelompokkan berdasarkan bulan dan hitung pelanggan uniknya
SELECT
    DATE_FORMAT(LastInvoiceDate, '%Y-%m') AS InvoiceMonth,
    COUNT(DISTINCT CustomerID) AS CustomerCount
FROM
    last_transactions
GROUP BY
    InvoiceMonth
ORDER BY
    InvoiceMonth;
  
create view negara_penghasil_customer as
(
    -- Kueri Pertama: Mengambil 5 negara teratas
    SELECT
        Country AS CountryGroup,
        COUNT(DISTINCT CustomerID) AS TotalCustomers
    FROM
        transaksi
    GROUP BY
        Country
    ORDER BY
        TotalCustomers DESC
    LIMIT 5
)

UNION ALL

(
    -- Kueri Kedua: Menghitung total pelanggan dari negara-negara lain
    SELECT
        'Other' AS CountryGroup,
        COUNT(DISTINCT CustomerID) AS TotalCustomers
    FROM
        transaksi
    WHERE
        Country NOT IN (
            SELECT Country
            FROM (
                SELECT Country, COUNT(DISTINCT CustomerID) AS customer_count
                FROM transaksi
                GROUP BY Country
                ORDER BY customer_count DESC
                LIMIT 5
            ) AS top_5_countries
        )
);