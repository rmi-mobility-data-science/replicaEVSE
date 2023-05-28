# Notes as I explore the data

## TODO:
* add charger type to load
* combine new county sampling before run simulation. 


### Commercial
* commercial travel in the trips tables have a person_id, however these person_ids are not in the population table. This is fine for PRIVATE_AUTO but will require modification of the code for COMMERCIAL trips. not a lot of modification since we barely use the population table now. We only use it to get the person_ids for people whos distination is in WA. 

### lengths of the full northwest region in millions
* pop_len = 58.222322
* trips_len = 159.624453 
* other_len = 49.674863
* pop_len = 14.889896
* trips_into_wa_len = 51.727268
* private auto trips into wa = 27.12446
* merged_len = 51.727268 
* unique persons who travelled to wa = 8.538399


### Note on vehicle type
* for the northwest region, we only have information on whether
the vehicle type is Heavy or Medium duty trucks. 

### COMMERCIAL
* there is only 1 person per trip in the commercial trips data
* None of the commercial drivers are in the population data
* I don't know how to get `stop_duration` out of trucks...

### Counties
* only LDV have county information from the population table. Commercial Does not. Using destinatino blockgroup to get counties associated with each person_id (trip for commercial)

### Charger availability
* need to include every charge but set the charging value to 0. 
