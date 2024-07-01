rm(list=ls())

#==============
# 1.0 Libraries
#==============
library(pacman)
p_load(tidyverse, lubridate, janitor, readxl, magrittr, odbc, DBI,stringr,anytime,data.table)
#=================
# 2.0 Import Data
#=================

# Getting the data cdu. Update the parameters where applicable, for example if also getting 2022H2 data add that in.
con <- dbConnect(odbc(), .connection_string = "driver={SQL Server};server=ops-reporting.ad.mlclife.com.au, 51847;database=BI_HUB_PRES;trusted_connection=true")
data <- dbGetQuery(con, "SELECT 
bf.BCID
,CaseNumber
,LegacyPolicyNumber AS POLICY_NUMBER
,BirthDate AS DATE_OF_BIRTH
,Gender AS GENDER
,bt.ClaimType AS 'C3_TYPE_OF_CLAIM'
,BenefitTypeGroup AS CLAIM_CHANNEL
,WaitingperiodDays AS CLAIM_WAITING_PERIOD
,case when BenefitTypeGroup = 'IP' then ROUND(BenefitAmountAnnual/12,0) else null end as CLAIM_MONTHLY_BENEFIT
,Outcome AS C2_CLAIM_STATUS
,BCstatus
,PrimaryMedicalCode AS C20_COMPANY_CAUSE_OF_CLAIM_INITIAL
,IncurredDate AS C4_CLAIM_EVENT_DATE
,NotificationDate AS C5_CLAIM_NOTIFICATION_DATE
,ClaimFormReceivedDate AS C6_CLAIM_FORM_RECEIVED_DATE
,InitialUnderAssessmentDate AS C7_CLAIM_RECEIVED_DATE_CALCULATED 
,CvInitialUnderAssessmentDate AS C7_CLAIM_RECEIVED_DATE
,BenefitClaimClosedDate AS C16_CLAIM_TERMINATED_DATE
,COALESCE(LicopInitialDecisionCommunicatedDate, InitialDecisionDate,InitialDeniedDate,BenefitClaimClosedDate) AS C11_CLAIM_FINALISED_DATE
,DecisionDate AS C8_CLAIM_WITHDRAWN_DATE
,ClaimReopenedDate AS C17_CLAIM_RE_OPENED_DATE
from [PRES].[vw_BenefitClaimedFact] bf
left join [PRES].[vw_BenefitClaimedDimension] bcd on bcd.BenefitClaimedId = bf.BenefitClaimedId
left join [PRES].[vw_PolicyDimension] pd on pd.PolicyId = bf.PolicyId
left join [PRES].[vw_ClaimantDimension] cld on cld.CaseId = bf.CaseId
left join [PRES].[vw_BenefitTypeDimension] bt on bt.BenefitId = bf.BenefitId
left join [PRES].[vw_DiagnosisDimension] d ON d.CaseId = bf.CaseId
left join [PRES].[vw_CaseDimension] cd on cd.CaseId = bf.CaseId")


payments <- dbGetQuery(con,"SELECT [Claim_Number]
      ,[CaseNumber]
      ,[Claim_Type]
      ,[Policy_Number]
      ,CAST([Claim_Payment_date] AS DATE) AS [Claim_Payment_date]
      ,[Claim_Payment_Ref]
      ,[Claim_Schedule_Ref]
      ,[Net_claim_amount_paid]
      ,[Payg_Tax_amount]
      ,[GrossAmount]
      ,[Claim_payment_status]
      ,[Payment_from_date]
      ,[Payment_to_date]
      ,[PolicyCommencementDate]
      ,[Source]
      ,[Business_Type]
  FROM [BI_HUB_PRES].[PRES].[TOTALCLAIMPAYMENT]
  WHERE Claim_Payment_date >= '2024-01-01' AND Claim_Payment_date <= '2024-06-30' ")

#results from Tony, data in long format
#results <- read_csv('inputs/Regression Testing/H1 2023/regression.csv', guess_max = 99999) %>% select (-c('DISCOVERY'))
#rename( 'C1_CLAIM_NUMBER_SEM' = 'C1_CLAIM_NUMBER...1', 'C1_CLAIM_NUMBER_DISCOVERY' = 'C1_CLAIM_NUMBER...2' )

#fsc data from discovery, data in regular submission format i.e. broad format
#fsc2022 <-  read_csv('inputs/Regression Testing/fsc2022H2updated.csv', guess_max = 99999)
fsc2024 <-  read_csv('inputs/Regression Testing/H1 2024/Reg claims input H1 2024.csv', guess_max = 99999)

#=================
# 3.0 Data Wrangling
#=================

#3.1 payments

payments_summarised <- payments %>% 
  filter(Claim_payment_status == 'PAID') %>% 
  group_by(CaseNumber,Claim_Type,Business_Type) %>% 
  summarise(AmountPaid = round(sum(GrossAmount))) %>% 
  ungroup()

#write_csv(payments_summarised, "outputs/temp.csv", na="")

#3.2 fsc 2023

fsc2024 <- fsc2024 %>% 
  mutate_at(vars('C4_CLAIM_EVENT_DATE','C5_CLAIM_NOTIFICATION_DATE', 'C7_CLAIM_RECEIVED_DATE', 'C16_CLAIM_TERMINATED_DATE','C6_CLAIM_FORM_RECEIVED_DATE',
                 'C11_CLAIM_FINALISED_DATE', 'C8_CLAIM_WITHDRAWN_DATE', 'C17_CLAIM_RE_OPENED_DATE', 'C7_CLAIM_RECEIVED_DATE'), funs(as.Date(.,"%Y-%m-%d")))

fsc2024 <- fsc2024 %>% 
  mutate_at(vars('C4_CLAIM_EVENT_DATE','C5_CLAIM_NOTIFICATION_DATE', 'C7_CLAIM_RECEIVED_DATE', 'C16_CLAIM_TERMINATED_DATE','C6_CLAIM_FORM_RECEIVED_DATE',
                 'C11_CLAIM_FINALISED_DATE', 'C8_CLAIM_WITHDRAWN_DATE', 'C17_CLAIM_RE_OPENED_DATE'), funs(format(.,"%d/%m/%Y"))) 


fsc2024_joined <- fsc2024 %>% 
  left_join(data %>%  select (BCID,CaseNumber), by = c('C1_CLAIM_NUMBER' = 'BCID'))

payments_fsc <- fsc2024_joined %>% 
  group_by(CaseNumber) %>% 
  summarise(AmountPaid = round(sum(C23_TOTAL_CLAIM_AMOUNT)))

#write_csv(fsc2022_joined, "outputs/temp2.csv", na="")

paymentsCheck <- payments_summarised %>% 
  left_join(payments_fsc, by= 'CaseNumber') %>% 
  rename(PaymentsBancs=AmountPaid.x, PaymentsSEM=AmountPaid.y) %>% 
  mutate(PaymentsCheck = PaymentsBancs==PaymentsSEM) %>% 
  mutate(PaymentsCheck = case_when(PaymentsBancs-PaymentsSEM == 1 |
                                     PaymentsSEM-PaymentsBancs == 1 |
                                     PaymentsBancs-PaymentsSEM == 2 |
                                     PaymentsSEM-PaymentsBancs == 2 ~ TRUE,
                                   TRUE ~ PaymentsCheck),
         Difference = PaymentsBancs - PaymentsSEM) %>% 
  filter(!is.na(PaymentsSEM) == TRUE)
#C-2017-000025
#C-2017-000040

paymentsCheck %>% filter(PaymentsCheck == 'FALSE') %>% 
  write_csv("outputs/fsc/H1 2024/paymentsCheckH12024.csv", na="")


#3.3 CV data
data2 <- data %>% 
  mutate_at(vars('C4_CLAIM_EVENT_DATE','C5_CLAIM_NOTIFICATION_DATE', 'C7_CLAIM_RECEIVED_DATE', 'C16_CLAIM_TERMINATED_DATE','C6_CLAIM_FORM_RECEIVED_DATE',
                 'C11_CLAIM_FINALISED_DATE', 'C8_CLAIM_WITHDRAWN_DATE', 'C17_CLAIM_RE_OPENED_DATE', 'DATE_OF_BIRTH', 'C7_CLAIM_RECEIVED_DATE'), funs(as.Date(.,"%Y-%m-%d"))) 

#data2 <- data2 %>% 
  #mutate_at(vars('C4_CLAIM_EVENT_DATE','C5_CLAIM_NOTIFICATION_DATE', 'C7_CLAIM_RECEIVED_DATE', 'C16_CLAIM_TERMINATED_DATE','C6_CLAIM_FORM_RECEIVED_DATE',
                 #'C11_CLAIM_FINALISED_DATE', 'C8_CLAIM_WITHDRAWN_DATE', 'C17_CLAIM_RE_OPENED_DATE', 'DATE_OF_BIRTH'), funs(format(.,"%d/%m/%Y"))) 

#data2 <- data2 %>% 
# mutate_at(vars('C4_CLAIM_EVENT_DATE','C5_CLAIM_NOTIFICATION_DATE', 'C7_CLAIM_RECEIVED_DATE', 'C16_CLAIM_TERMINATED_DATE','C6_CLAIM_FORM_RECEIVED_DATE',
#      'C11_CLAIM_FINALISED_DATE', 'C8_CLAIM_WITHDRAWN_DATE', 'C17_CLAIM_RE_OPENED_DATE', 'DATE_OF_BIRTH'), funs(format(.,"%d/%m/%Y"))) 

#####################################################################################################################################################################

#final <- fsc2023_joined %>% 
# left_join(data2, by = c('C1_CLAIM_NUMBER' = 'BCID'))





#####################################################################################################################################################################
# gather code
data_reshape <- data2 %>%
  gather('ColumnName', 'Value', `POLICY_NUMBER`:`C17_CLAIM_RE_OPENED_DATE`)


# no duplicates but just in case
data_reshape <- unique(data_reshape)



fsc2024_reshape <- fsc2024_joined %>% 
  select(-c(START_DATE,START_DATE_INT,END_DATE,END_DATE_INT)) %>% 
  select(c(8,36,1:35)) %>% 
  gather('ColumnName', 'Value', `POLICY_NUMBER`:`C30_WORKERS_COMPENSATION_INVOLVEMENT`)

# no duplicates but just in case
fsc2023_reshape <- unique(fsc2023_reshape)

final <- fsc2023_reshape %>% 
  left_join(data_reshape, by= c('C1_CLAIM_NUMBER' = 'BCID', 'ColumnName' = 'ColumnName')) %>% 
  #rename(CvValueDiscovery = Value) %>% 
  #mutate_at(vars('SEM','DISCOVERY'), funs(dmy)) %>% 
  mutate(Check = Value.x==Value.y) %>% 
  select(c(C1_CLAIM_NUMBER, ColumnName, FSCvalue = Value.x ,ORMvalue=Value.y, Check))

write_csv(final, 'Outputs/fsc/H2 2023/RegressionReport2023H2.csv', na="")
