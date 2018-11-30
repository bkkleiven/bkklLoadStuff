-- =============================================
-- Author 		: Shahila Retnadhas
-- Create Date	: 10.08.2018
-- Description	: Upsert notification data from 
--                staging to primary tables
-- =============================================
CREATE PROCEDURE [dbo].[process_notifications_staging_table]
AS
BEGIN

	SET NOCOUNT ON
	
	DELETE DUP_REC FROM ( SELECT *, RN=ROW_NUMBER() OVER (PARTITION BY NOTIFNO ORDER BY [DateLastChange] DESC)
							FROM [dbo].[Notifications_Staging] 
						) DUP_REC WHERE RN > 1

	-- DECLARE a temp table to hold all new or updated Notifications id's (NotifNo)
	DECLARE @Notifications TABLE (NotifNo nvarchar(12))

	INSERT INTO @Notifications
	SELECT DISTINCT [NotifNo] FROM [dbo].[Notifications_Staging]

	/*  Remove records that has an update */
	DELETE FROM [dbo].[Notifications] WHERE [NotifNo] IN (SELECT [NotifNo] FROM @Notifications)
	
	/*  Insert all records into the notification operation table */
INSERT INTO [dbo].[Notifications]
	SELECT [NotifNo]
	  ,CAST(DATEADD(ms, CONVERT(bigint, SUBSTRING([DateLastChange],7, PATINDEX('%)%', [DateLastChange]) - 7))%1000, DATEADD(SS, (CONVERT(bigint, SUBSTRING([DateLastChange],7, PATINDEX('%)%', [DateLastChange]) - 7))/1000)%3600, DATEADD(hh, CONVERT(bigint, SUBSTRING([DateLastChange],7, PATINDEX('%)%', [DateLastChange]) - 7))/3600000, '19700101' ) )) as DateTime2)  as [DateLastChange]	
      ,[NotificationType]
      ,[PlanningPlant]
      ,[Iwerk]
      ,[IwerkTxt]
      ,[Swerk]
      ,[SwerkTxt]
      ,[Qmart]
	  ,CAST(CASE WHEN ([Aedat] IS NULL OR TRIM([Aedat]) = '') THEN NULL
				 WHEN ([Aezeit] IS NULL OR TRIM([Aezeit]) = '' OR SUBSTRING([Aezeit],1,4) = 'PT24' OR SUBSTRING([Aezeit],1,3) = 'PT ' OR SUBSTRING([Aezeit],1,2) != 'PT') THEN CAST(CAST(DATEADD(hh, CONVERT(bigint, SUBSTRING([Aedat],7, PATINDEX('%)%', [Aedat]) - 7))/3600000, '19700101' ) as Date) as varchar(10)) + ' 00:00:00'
				 ELSE CAST(CAST(DATEADD(hh, CONVERT(bigint, SUBSTRING([Aedat],7, PATINDEX('%)%', [Aedat]) - 7))/3600000, '19700101' ) as Date) as varchar(10)) + ' ' + SUBSTRING([Aezeit],3,2) + ':' + SUBSTRING([Aezeit],6,2) + ':' + SUBSTRING([Aezeit],9,2)
			END AS DateTime2) AS [Aedat]
	  ,[Aezeit]
      ,[Auswk]
      ,[Auswkt]
	  ,CAST(DATEADD(ms, CONVERT(bigint, SUBSTRING([Ltrmn],7, PATINDEX('%)%', [Ltrmn]) - 7))%1000, DATEADD(SS, (CONVERT(bigint, SUBSTRING([Ltrmn],7, PATINDEX('%)%', [Ltrmn]) - 7))/1000)%3600, DATEADD(hh, CONVERT(bigint, SUBSTRING([Ltrmn],7, PATINDEX('%)%', [Ltrmn]) - 7))/3600000, '19700101' ) )) as DateTime2)  as [Ltrmn]
	  ,CAST(CASE WHEN ([Qmdat] IS NULL OR TRIM([Qmdat]) = '') THEN NULL
				 WHEN ([Mzeit] IS NULL OR TRIM([Mzeit]) = '' OR SUBSTRING([Mzeit],1,4) = 'PT24' OR SUBSTRING([Mzeit],1,3) = 'PT ' OR SUBSTRING([Mzeit],1,2) != 'PT') THEN CAST(CAST(DATEADD(hh, CONVERT(bigint, SUBSTRING([Qmdat],7, PATINDEX('%)%', [Qmdat]) - 7))/3600000, '19700101' ) as Date) as varchar(10)) + ' 00:00:00'
				 ELSE CAST(CAST(DATEADD(hh, CONVERT(bigint, SUBSTRING([Qmdat],7, PATINDEX('%)%', [Qmdat]) - 7))/3600000, '19700101' ) as Date) as varchar(10)) + ' ' + SUBSTRING([Mzeit],3,2) + ':' + SUBSTRING([Mzeit],6,2) + ':' + SUBSTRING([Mzeit],9,2)
			END AS DateTime2) AS [Qmdat]
      ,[Mzeit]
	  ,CAST(CASE WHEN ([Erdat] IS NULL OR TRIM([Erdat]) = '') THEN NULL
				 WHEN ([Erzeit] IS NULL OR TRIM([Erzeit]) = '' OR SUBSTRING([Erzeit],1,4) = 'PT24' OR SUBSTRING([Erzeit],1,3) = 'PT ' OR SUBSTRING([Erzeit],1,2) != 'PT') THEN CAST(CAST(DATEADD(hh, CONVERT(bigint, SUBSTRING([Erdat],7, PATINDEX('%)%', [Erdat]) - 7))/3600000, '19700101' ) as Date) as varchar(10)) + ' 00:00:00'
				 ELSE CAST(CAST(DATEADD(hh, CONVERT(bigint, SUBSTRING([Erdat],7, PATINDEX('%)%', [Erdat]) - 7))/3600000, '19700101' ) as Date) as varchar(10)) + ' ' + SUBSTRING([Erzeit],3,2) + ':' + SUBSTRING([Erzeit],6,2) + ':' + SUBSTRING([Erzeit],9,2)
			END AS DateTime2) AS [Erdat]	  
      ,[Erzeit]
	  ,CAST(CASE WHEN ([Qmdab] IS NULL OR TRIM([Qmdab]) = '') THEN NULL
				 WHEN ([Qmzab] IS NULL OR TRIM([Qmzab]) = '' OR SUBSTRING([Qmzab],1,4) = 'PT24' OR SUBSTRING([Qmzab],1,3) = 'PT ' OR SUBSTRING([Qmzab],1,2) != 'PT') THEN CAST(CAST(DATEADD(hh, CONVERT(bigint, SUBSTRING([Qmdab],7, PATINDEX('%)%', [Qmdab]) - 7))/3600000, '19700101' ) as Date) as varchar(10)) + ' 00:00:00'
				 ELSE CAST(CAST(DATEADD(hh, CONVERT(bigint, SUBSTRING([Qmdab],7, PATINDEX('%)%', [Qmdab]) - 7))/3600000, '19700101' ) as Date) as varchar(10)) + ' ' + SUBSTRING([Qmzab],3,2) + ':' + SUBSTRING([Qmzab],6,2) + ':' + SUBSTRING([Qmzab],9,2)
			END AS DateTime2) AS [Qmdab]	  
      ,[Qmzab]
	  ,CAST(CASE WHEN ([Bezdt] IS NULL OR TRIM([Bezdt]) = '') THEN NULL
				 WHEN ([Bezur] IS NULL OR TRIM([Bezur]) = '' OR SUBSTRING([Bezur],1,4) = 'PT24' OR SUBSTRING([Bezur],1,3) = 'PT ' OR SUBSTRING([Bezur],1,2) != 'PT') THEN CAST(CAST(DATEADD(hh, CONVERT(bigint, SUBSTRING([Bezdt],7, PATINDEX('%)%', [Bezdt]) - 7))/3600000, '19700101' ) as Date) as varchar(10)) + ' 00:00:00'
				 ELSE CAST(CAST(DATEADD(hh, CONVERT(bigint, SUBSTRING([Bezdt],7, PATINDEX('%)%', [Bezdt]) - 7))/3600000, '19700101' ) as Date) as varchar(10)) + ' ' + SUBSTRING([Bezur],3,2) + ':' + SUBSTRING([Bezur],6,2) + ':' + SUBSTRING([Bezur],9,2)
			END AS DateTime2) AS [Bezdt]	  
      ,[Bezur]
      ,SUBSTRING([Aufnr], PATINDEX('%[^0]%', [Aufnr]), LEN([Aufnr])) AS [Aufnr]
      ,[Priok]
      ,[Qmtxt]
      ,[Ingrp]
      ,[Arbpl]
      ,[Arbplwerk]
      ,[ArbplwerkTxt]
      ,[Msaus]
      ,[Auszt]
	  ,CAST(CASE WHEN ([Ausvn] IS NULL OR TRIM([Ausvn]) = '') THEN NULL
				 WHEN ([Auztv] IS NULL OR TRIM([Auztv]) = '' OR SUBSTRING([Auztv],1,4) = 'PT24' OR SUBSTRING([Auztv],1,3) = 'PT ' OR SUBSTRING([Auztv],1,2) != 'PT') THEN CAST(CAST(DATEADD(hh, CONVERT(bigint, SUBSTRING([Ausvn],7, PATINDEX('%)%', [Ausvn]) - 7))/3600000, '19700101' ) as Date) as varchar(10)) + ' 00:00:00'
				 ELSE CAST(CAST(DATEADD(hh, CONVERT(bigint, SUBSTRING([Ausvn],7, PATINDEX('%)%', [Ausvn]) - 7))/3600000, '19700101' ) as Date) as varchar(10)) +' ' + SUBSTRING([Auztv],3,2) + ':' + SUBSTRING([Auztv],6,2) + ':' + SUBSTRING([Auztv],9,2)
			END AS DateTime2) AS [Ausvn]
      ,[Auztv]
	  ,CAST(CASE WHEN ([Ausbs] IS NULL OR TRIM(Ausbs) = '') THEN NULL
				 WHEN ([Auztb] IS NULL OR TRIM([Auztb]) = '' OR SUBSTRING([Auztb],1,4) = 'PT24' OR SUBSTRING([Auztb],1,3) = 'PT ' OR SUBSTRING([Auztb],1,2) != 'PT') THEN CAST(CAST(DATEADD(hh, CONVERT(bigint, SUBSTRING([Ausbs],7, PATINDEX('%)%', [Ausbs]) - 7))/3600000, '19700101' ) as Date) as varchar(10)) + ' 00:00:00'
				 ELSE CAST(CAST(DATEADD(hh, CONVERT(bigint, SUBSTRING([Ausbs],7, PATINDEX('%)%', [Ausbs]) - 7))/3600000, '19700101' ) as Date) as varchar(10)) + ' ' + SUBSTRING([Auztb],3,2) + ':' + SUBSTRING([Auztb],6,2) + ':' + SUBSTRING([Auztb],9,2)
			END AS DateTime2) AS [Ausbs]
      ,[Auztb]
      ,[Zzcritfail]
      ,[Innam]
      ,[Longtxt]
      ,[Actvtxt]
      ,[Fecod]
      ,[FecodTxt]
      ,[Urcod]
      ,[UrcodTxt]
      ,[Oteil]
      ,[OteilTxt]
      ,[Sysstatus]
      ,[Userstatus]
      ,[Beber]
      ,[Tplnr]
      ,[Pltxt]
      ,[Stort]
      ,[Abckz]
      ,[Eqart]
      ,[EqartTxt]
      ,[Rbnr]
      ,[Rbnrx]
      ,[CritCriticality]
      ,[ClassificationPlant]
      ,[CritMainFunction]
      ,[CritSubFunction]
      ,[CritRedundancy]
      ,[CritSafety]
      ,[CritProduction]
      ,[CritCost]
      ,[McConceptNoConcern]
      ,[McConceptNoPlant]
      ,[McRepairStrategy]
      ,[McCriticalSpares]
      ,[McSparePartRequirement]
      ,[ControlClass]
      ,[CritFailHse]
      ,[CritContainment]
      ,[CritContainmentCategory]
      ,[Equnr]
      ,[Eqkt]
      ,[Mngrps]
      ,[Mncods]
      ,[MngrpsTxt]
      ,[MncodsTxt]
	FROM [dbo].[Notifications_Staging]
    
END