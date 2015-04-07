select concat('INSERT INTO regions VALUES(',regionID,',\'',regionName, '\')') from eve_static.mapRegions limit 10000000;

select concat('INSERT INTO systems VALUES(',solarSystemID, ',', regionID, ',\'', solarSystemName,'\')') from eve_static.mapSolarSystems limit 10000000;

select concat('INSERT INTO types VALUES(',typeID, ',\'', replace(typeName, '\'', '\\\''),'\')') from eve_static.invTypes where marketGroupID is not null limit 1000000000;