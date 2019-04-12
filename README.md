# Crimes Application

Application do the following:
- From CSV files gather the crime records with non-empty crimeID.
- Group resulting list by coordinate pairs.
- Sort coordinate pairs by total number of crimes, most repeated crime locations first.
- Print out top 5 crime locations, each with list of associated theft incidents, for example:
-----------------------------------
(0.0234567,0.0345678): 246
Thefts:
theft1
theft2
....
theftN
-------------------------------------
(0.1234567,0.1345678): 210
Thefts:
theft1
theft2
....
theftN
-------------------------------------