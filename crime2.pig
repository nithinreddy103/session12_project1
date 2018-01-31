Data = load '/home/acadgild/Downloads/Crimes_-_2001_to_present.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX') AS (ID: chararray, Case_Num: chararray, date: chararray, block: chararray, IUCR: chararray, type: chararray, desc: chararray, arrest:chararray, domestic :chararray, beat:chararray, district:chararray, ward:chararray, area:chararray, FBIcode:chararray, X:chararray, Y:chararray, year: int , updated_on : chararray, lat: chararray, long: chararray, location:chararray);
GroupData = group Data by FBIcode;
Crimecount =  foreach GroupData GENERATE group as FBIcode,COUNT(Data.FBIcode);
FilterData = FILTER Crimecount BY FBIcode MATCHES '32';
DUMP FilterData;
