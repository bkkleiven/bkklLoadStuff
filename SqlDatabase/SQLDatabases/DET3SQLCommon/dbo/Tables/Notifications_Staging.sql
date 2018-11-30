﻿CREATE TABLE [dbo].[Notifications_Staging](
	[NotifNo] [nvarchar](12) NOT NULL,
	[DateLastChange] [nvarchar](40) NOT NULL,
	[NotificationType] [nchar](2) NULL,
	[PlanningPlant] [nvarchar](4) NULL,
	[Iwerk] [nchar](4) NULL,
	[IwerkTxt] [nvarchar](30) NULL,
	[Swerk] [nchar](4) NULL,
	[SwerkTxt] [nvarchar](30) NULL,
	[Qmart] [nchar](2) NULL,
	[Aedat] [nvarchar](40) NULL,
	[Aezeit] [nvarchar](12) NULL,
	[Auswk] [nchar](1) NULL,
	[Auswkt] [nvarchar](40) NULL,
	[Ltrmn] [nvarchar](40) NULL,
	[Qmdat] [nvarchar](40) NULL,
	[Mzeit] [nvarchar](12) NULL,
	[Erdat] [nvarchar](40) NULL,
	[Erzeit] [nvarchar](12) NULL,
	[Qmdab] [nvarchar](40) NULL,
	[Qmzab] [nvarchar](12) NULL,
	[Bezdt] [nvarchar](40) NULL,
	[Bezur] [nvarchar](12) NULL,
	[Aufnr] [nvarchar](12) NULL,
	[Priok] [nchar](1) NULL,
	[Qmtxt] [nvarchar](40) NULL,
	[Ingrp] [nchar](3) NULL,
	[Arbpl] [nvarchar](8) NULL,
	[Arbplwerk] [nchar](4) NULL,
	[ArbplwerkTxt] [nvarchar](30) NULL,
	[Msaus] [nvarchar](5) NULL,
	[Auszt] [float] NULL,
	[Ausvn] [nvarchar](40) NULL,
	[Auztv] [nvarchar](12) NULL,
	[Ausbs] [nvarchar](40) NULL,
	[Auztb] [nvarchar](12) NULL,
	[Zzcritfail] [nvarchar](1) NULL,
	[Innam] [nvarchar](18) NULL,
	[Longtxt] [text] NULL,
	[Actvtxt] [text] NULL,
	[Fecod] [nchar](4) NULL,
	[FecodTxt] [nvarchar](80) NULL,
	[Urcod] [nchar](4) NULL,
	[UrcodTxt] [nvarchar](80) NULL,
	[Oteil] [nchar](4) NULL,
	[OteilTxt] [nvarchar](80) NULL,
	[Sysstatus] [nvarchar](40) NULL,
	[Userstatus] [nvarchar](40) NULL,
	[Beber] [nchar](3) NULL,
	[Tplnr] [nvarchar](40) NULL,
	[Pltxt] [nvarchar](40) NULL,
	[Stort] [nvarchar](10) NULL,
	[Abckz] [nchar](1) NULL,
	[Eqart] [nvarchar](10) NULL,
	[EqartTxt] [nvarchar](20) NULL,
	[Rbnr] [nvarchar](9) NULL,
	[Rbnrx] [nvarchar](30) NULL,
	[CritCriticality] [nvarchar](30) NULL,
	[ClassificationPlant] [nvarchar](30) NULL,
	[CritMainFunction] [nvarchar](30) NULL,
	[CritSubFunction] [nvarchar](30) NULL,
	[CritRedundancy] [nvarchar](30) NULL,
	[CritSafety] [nvarchar](30) NULL,
	[CritProduction] [nvarchar](30) NULL,
	[CritCost] [nvarchar](30) NULL,
	[McConceptNoConcern] [nvarchar](30) NULL,
	[McConceptNoPlant] [nvarchar](30) NULL,
	[McRepairStrategy] [nvarchar](30) NULL,
	[McCriticalSpares] [nvarchar](30) NULL,
	[McSparePartRequirement] [nvarchar](30) NULL,
	[ControlClass] [nvarchar](30) NULL,
	[CritFailHse] [nvarchar](30) NULL,
	[CritContainment] [nvarchar](30) NULL,
	[CritContainmentCategory] [nvarchar](30) NULL,
	[Equnr] [nvarchar](18) NULL,
	[Eqkt] [nvarchar](40) NULL,
	[Mngrps] [nvarchar](255) NULL,
	[Mncods] [nvarchar](255) NULL,
	[MngrpsTxt] [nvarchar](1024) NULL,
	[MncodsTxt] [nvarchar](1024) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]