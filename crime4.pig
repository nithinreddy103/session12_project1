Data = load '/home/acadgild/Downloads/Crimes_-_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX') AS (ID: chararray, Case_Num: chararray, date: chararray, block: chararray, IUCR: chararray, type: chararray, desc: chararray, arrest:chararray, domestic :chararray, beat:chararray, district:chararray, ward:chararray, area:chararray, FBIcode:chararray, X:chararray, Y:chararray, year: int , updated_on : chararray, lat: chararray, long: chararray, location:chararray);
crimedate = FOREACH Data GENERATE ToDate(SUBSTRING(date,0,10), 'MM/dd/yyyy') as (dt:datetime);
crimedate1 = FILTER crimedate BY DaysBetween(dt,(datetime)ToDate('10/01/2014', 'MM/dd/yyyy')) >=(long)0;
crimedate2 = FILTER crimedate1 BY DaysBetween(dt,(datetime)ToDate('10/01/2015', 'MM/dd/yyyy')) <=(long)0;
crimecount = group crimedate2 all;
count = FOREACH crimecount GENERATE COUNT(crimedate2);
Store count into '/home/acadgild/session12/c41' USING PigStorage(',');
Dump count;
