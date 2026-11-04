WITH customer_spending AS (
    -- 1. Hitung total spending ALL customer (tanpa filter >= 3 dulu)
    -- Ini dipakai untuk menghitung rata-rata kota yang akurat (2625.0)
    SELECT 
        c.customer_id,
        c.customer_name,
        c.city,
        COUNT(o.order_id) AS transaction_count,
        SUM(o.total_amount) AS total_spending
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    WHERE c.city IS NOT NULL
    GROUP BY c.customer_id, c.customer_name, c.city
),
filtered_customers AS (
    -- 2. Ambil hanya yang transaksi >= 3 untuk tampilan akhir
    SELECT * 
    FROM customer_spending
    WHERE transaction_count >= 3
)
SELECT 
    fc.customer_name,
    fc.city,
    fc.transaction_count,
    fc.total_spending,
    
    -- CORRELATED SUBQUERY: Menghitung rata-rata dari ALL customer di kota tsb
    (
        SELECT AVG(cs.total_spending)
        FROM customer_spending cs
        WHERE cs.city = fc.city
    ) AS avg_city_spending,

    -- KATEGORISASI: Langsung bandingkan dengan Correlated Subquery di atas
    CASE 
        WHEN fc.total_spending > 2 * (
            SELECT AVG(cs.total_spending) 
            FROM customer_spending cs 
            WHERE cs.city = fc.city
        ) THEN 'HIGH SPENDER'
        
        WHEN fc.total_spending < 0.5 * (
            SELECT AVG(cs.total_spending) 
            FROM customer_spending cs 
            WHERE cs.city = fc.city
        ) THEN 'LOW SPENDER'
        
        ELSE 'AVERAGE'
    END AS spending_category

FROM filtered_customers fc
ORDER BY 
    fc.total_spending DESC, 
    fc.city ASC;