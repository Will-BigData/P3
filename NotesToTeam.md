To download everything
wget -r -np -nH --cut-dirs=3 -R "index.html*" ftp:<URL HERE>

Unzip Everything
find /path/to/folder -name "*.zip" -exec unzip -d "{}_unzipped" {} \;

Move everything to one place
find . -type d -name "*_unzipped" -exec sh -c 'find "$1" -type f -exec mv {} ~/p3_data_2020/ \;' sh {} \;


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