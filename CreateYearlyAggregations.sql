/****** Script for SelectTopNRows command from SSMS  ******/
SELECT rs.DateYear, rs.Keyword, rs.OccurenceCount 
INTO dbo.hntitlesKeywordsYearlyTop50
FROM (SELECT rs.DateYear, rs.Keyword, rs.OccurenceCount, Rank() over (PARTITION BY DateYear ORDER BY OccurenceCount DESC) AS Rank
	FROM (SELECT YEAR([Date]) AS DateYear
      ,[Keyword]
      ,SUM([OccurenceCount]) AS OccurenceCount
	 FROM [dbo].[hntitlesKeywords]
	 GROUP BY YEAR([Date]), Keyword
	 ) rs 
) rs WHERE Rank <= 50 ORDER BY DateYear, OccurenceCount DESC