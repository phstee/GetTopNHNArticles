/****** Script for SelectTopNRows command from SSMS  ******/
SELECT rs.DateYearMonth, rs.Keyword, rs.OccurenceCount 
INTO dbo.hntitlesKeywordsMonthlyTop30
FROM (SELECT rs.DateYearMonth, rs.Keyword, rs.OccurenceCount, Rank() over (PARTITION BY DateYearMonth ORDER BY OccurenceCount DESC) AS Rank
	FROM (SELECT CONCAT (datepart (yy,[Date]), FORMAT ([Date],'MM')) AS DateYearMonth
      ,[Keyword]
      ,SUM([OccurenceCount]) AS OccurenceCount
	 FROM [dbo].[hntitlesKeywords]
	 GROUP BY CONCAT (datepart (yy,[Date]), FORMAT ([Date],'MM')), Keyword
	 ) rs 
) rs WHERE Rank <= 30 ORDER BY DateYearMonth, OccurenceCount DESC
