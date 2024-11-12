To download everything
wget -r -np -nH --cut-dirs=3 -R "index.html*" ftp:<URL HERE, no https, replace www with ftp as well>

Unzip Everything
find /path/to/folder -name "*.zip" -exec unzip -d "{}_unzipped" {} \;

Move everything to one place
find . -type d -name "*_unzipped" -exec sh -c 'find "$1" -type f -exec mv {} ~/p3_data_2020/ \;' sh {} \;

Move everything from sub directory to main
mv /path/to/mainfolder/* /path/to/mainfolder/

Divide everything up examples
hdfs dfs -mv /user/jimmy/p3/*geo* /user/jimmy/p3/GeoHeader


Teams:
1) Breakout Study Room 
2020
Jimmy,
Nathan,
Joshua,
Nishad
Abraham

2) Breakout Study Room 2
Jacob
Orlando
Zak
Dejen

3) Breakout Study Room 3
Michael
Dane
Kenneth
James