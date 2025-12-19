package com.example.demo.repository;

import com.example.demo.model.User;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public interface UserRepository extends JpaRepository<User, Long> {
    Optional<User> findByEmail(String email);

    // Long-running stuck query for testing query performance monitoring
    @Query(value = "WAITFOR DELAY '00:05:00'; SELECT * FROM users", nativeQuery = true)
    List<User> executeLongRunningQuery();

    // Slow Query 1: Complex join on AdventureWorks2022 Sales tables with 1-minute delay
    @Query(value = """
        WAITFOR DELAY '00:01:00';

        SELECT
            soh.SalesOrderID,
            soh.OrderDate,
            soh.TotalDue,
            p.Name AS ProductName,
            sod.OrderQty,
            sod.UnitPrice,
            c.FirstName + ' ' + c.LastName AS CustomerName
        FROM AdventureWorks2022.Sales.SalesOrderHeader soh
        INNER JOIN AdventureWorks2022.Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
        INNER JOIN AdventureWorks2022.Production.Product p ON sod.ProductID = p.ProductID
        INNER JOIN AdventureWorks2022.Sales.Customer cust ON soh.CustomerID = cust.CustomerID
        INNER JOIN AdventureWorks2022.Person.Person c ON cust.PersonID = c.BusinessEntityID
        WHERE soh.OrderDate >= '2013-01-01'
        AND soh.TotalDue > 5000.00
        AND p.ListPrice > 100.00
        ORDER BY soh.TotalDue DESC
        """, nativeQuery = true)
    List<Map<String, Object>> executeSlowSalesQuery();

    // Active Running Query: 90-second delay to ensure capture across multiple scrape intervals
    // This guarantees the query will be visible in sys.dm_exec_requests when collector scrapes
    @Query(value = """
        WAITFOR DELAY '00:01:30';

        SELECT
            soh.SalesOrderID,
            soh.OrderDate,
            soh.TotalDue,
            p.Name AS ProductName,
            sod.OrderQty,
            sod.UnitPrice,
            c.FirstName + ' ' + c.LastName AS CustomerName
        FROM AdventureWorks2022.Sales.SalesOrderHeader soh
        INNER JOIN AdventureWorks2022.Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
        INNER JOIN AdventureWorks2022.Production.Product p ON sod.ProductID = p.ProductID
        INNER JOIN AdventureWorks2022.Sales.Customer cust ON soh.CustomerID = cust.CustomerID
        INNER JOIN AdventureWorks2022.Person.Person c ON cust.PersonID = c.BusinessEntityID
        WHERE soh.OrderDate >= '2013-01-01'
        AND soh.TotalDue > 5000.00
        AND p.ListPrice > 100.00
        ORDER BY soh.TotalDue DESC
        """, nativeQuery = true)
    List<Map<String, Object>> executeActiveSalesQuery();

    // Slow Query 2: Complex aggregation on AdventureWorks2022 Production tables with 5-minute delay
    @Query(value = """
        WAITFOR DELAY '00:05:00';

        SELECT
            pc.Name AS CategoryName,
            p.ProductNumber,
            p.Name AS ProductName,
            p.StandardCost,
            p.ListPrice,
            (p.ListPrice - p.StandardCost) AS ProfitMargin,
            SUM(pi.Quantity) AS TotalInventory,
            AVG(pod.OrderQty) AS AvgOrderQuantity
        FROM AdventureWorks2022.Production.Product p
        INNER JOIN AdventureWorks2022.Production.ProductSubcategory psc ON p.ProductSubcategoryID = psc.ProductSubcategoryID
        INNER JOIN AdventureWorks2022.Production.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
        LEFT JOIN AdventureWorks2022.Production.ProductInventory pi ON p.ProductID = pi.ProductID
        LEFT JOIN AdventureWorks2022.Purchasing.PurchaseOrderDetail pod ON p.ProductID = pod.ProductID
        WHERE p.FinishedGoodsFlag = 1
        AND p.ListPrice > 50.00
        AND pc.Name IN ('Bikes', 'Components', 'Clothing')
        GROUP BY pc.Name, p.ProductNumber, p.Name, p.StandardCost, p.ListPrice
        HAVING SUM(pi.Quantity) > 100
        ORDER BY ProfitMargin DESC
        """, nativeQuery = true)
    List<Map<String, Object>> executeSlowProductQuery();

    // NEW: Active Query Pattern 1 - Long-running aggregation (2 minutes)
    // Simulates expensive analytical query that will be captured as active
    @Query(value = """
        /* APM_ACTIVE_AGGREGATION */
        WAITFOR DELAY '00:02:00';

        SELECT
            YEAR(soh.OrderDate) AS OrderYear,
            MONTH(soh.OrderDate) AS OrderMonth,
            COUNT(DISTINCT soh.SalesOrderID) AS OrderCount,
            COUNT(DISTINCT soh.CustomerID) AS CustomerCount,
            SUM(soh.TotalDue) AS TotalRevenue,
            AVG(soh.TotalDue) AS AvgOrderValue
        FROM AdventureWorks2022.Sales.SalesOrderHeader soh
        WHERE soh.OrderDate >= '2011-01-01'
        GROUP BY YEAR(soh.OrderDate), MONTH(soh.OrderDate)
        ORDER BY OrderYear DESC, OrderMonth DESC
        """, nativeQuery = true)
    List<Map<String, Object>> executeLongAggregationQuery();

    // NEW: Active Query Pattern 2 - Blocking query with explicit lock (90 seconds)
    // Creates LCK_M_X wait type for testing blocking scenarios
    @Query(value = """
        /* APM_BLOCKING_PATTERN */
        BEGIN TRANSACTION;

        SELECT TOP 10 *
        FROM AdventureWorks2022.Person.Person WITH (UPDLOCK, HOLDLOCK)
        WHERE BusinessEntityID BETWEEN 1 AND 100;

        WAITFOR DELAY '00:01:30';

        COMMIT TRANSACTION;
        """, nativeQuery = true)
    List<Map<String, Object>> executeBlockingQuery();

    // NEW: Active Query Pattern 3 - CPU-intensive query (90 seconds)
    // High CPU time for testing performance monitoring - FIXED to avoid OOM
    @Query(value = """
        /* APM_CPU_INTENSIVE */
        DECLARE @counter INT = 0;
        DECLARE @result FLOAT = 0;

        WHILE @counter < 500000
        BEGIN
            SET @result = SQRT(POWER(@counter, 2) + LOG(@counter + 1));
            SET @counter = @counter + 1;
        END

        SELECT TOP 50
            p.ProductID,
            p.Name,
            UPPER(p.Name) AS UpperName,
            LOWER(p.Name) AS LowerName,
            REVERSE(p.Name) AS ReverseName,
            LEN(p.Name) AS NameLength
        FROM AdventureWorks2022.Production.Product p
        WHERE p.ProductID <= 100
        ORDER BY p.ProductID
        """, nativeQuery = true)
    List<Map<String, Object>> executeCpuIntensiveQuery();

    // NEW: Active Query Pattern 4 - I/O intensive query (90 seconds)
    // High logical reads for testing I/O monitoring - FIXED to avoid OOM
    @Query(value = """
        /* APM_IO_INTENSIVE */
        WAITFOR DELAY '00:01:30';

        SELECT TOP 100
            soh.SalesOrderID,
            soh.OrderDate,
            soh.TotalDue,
            p.Name as ProductName
        FROM AdventureWorks2022.Sales.SalesOrderHeader soh
        INNER JOIN AdventureWorks2022.Sales.SalesOrderDetail sod ON soh.SalesOrderID = sod.SalesOrderID
        INNER JOIN AdventureWorks2022.Production.Product p ON sod.ProductID = p.ProductID
        WHERE soh.TotalDue > 1000
        ORDER BY soh.TotalDue DESC
        """, nativeQuery = true)
    List<Map<String, Object>> executeIoIntensiveQuery();

    // SINGLE FOCUSED QUERY: Person UPDLOCK with blocking and execution plan
    // This query:
    // 1. Holds UPDLOCK on Person.Person (creates blocking)
    // 2. Takes 90 seconds (captured as active query)
    // 3. Has execution plan with multiple operators
    // 4. Same query_hash for both slow and active captures
    @Query(value = """
        /* PERSON_UPDLOCK_WITH_PLAN */
        BEGIN TRANSACTION;
        SELECT
            p.BusinessEntityID,
            p.PersonType,
            p.FirstName,
            p.LastName,
            (SELECT TOP 1 ea.EmailAddress
             FROM AdventureWorks2022.Person.EmailAddress ea
             WHERE ea.BusinessEntityID = p.BusinessEntityID) AS Email,
            (SELECT TOP 1 pp.PhoneNumber
             FROM AdventureWorks2022.Person.PersonPhone pp
             WHERE pp.BusinessEntityID = p.BusinessEntityID) AS Phone
        FROM AdventureWorks2022.Person.Person p WITH(UPDLOCK)
        WHERE p.BusinessEntityID BETWEEN 1 AND 50
        ORDER BY p.BusinessEntityID;
        WAITFOR DELAY '00:01:30';
        COMMIT TRANSACTION;
        """, nativeQuery = true)
    List<Map<String, Object>> executePersonUpdlockQuery();
}
