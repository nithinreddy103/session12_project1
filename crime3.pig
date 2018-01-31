Data = load '/home/acadgild/Downloads/Crimes_-_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX') AS (ID: chararray, Case_Num: chararray, date: chararray, block: chararray, IUCR: chararray, type: chararray, desc: chararray, arrest:chararray, domestic :chararray, beat:chararray, district:chararray, ward:chararray, area:chararray, FBIcode:chararray, X:chararray, Y:chararray, year: int , updated_on : chararray, lat: chararray, long: chararray, location:chararray);
FilterData = FILTER Data BY type == 'THEFT';
GroupData = GROUP FilterData BY district;
crimecount = foreach GroupData GENERATE group as district,COUNT(FilterData.Case_Num);
Store crimecount into '/home/acadgild/session12/c31' USING PigStorage(',');
DUMP crimecount;
