# Notes as I explore the data

## TODO:
* gut check: vmt vs efficiency to get total power needed. 
    - run on state wide values
    - could be in the merge, don't need that now.

* rerun the model with updated stock data from Gerard. 
    - needs to be uploaded from Downloads
    - bug idea: shared users between segments?

* run PHEV using logic in the sharepoint file.

* find commit where there was a large plot
    - see this: https://stackoverflow.com/questions/10622179/how-to-find-identify-large-commits-in-git-history

* Update this: Summary statistic X% of charging takes place at home, Y% charges public Z% charges at work. Use charge_id and wrap all work together. 

* ~~Add CommercialL: rewrite load curve plotting code to take in load curves and then try `plt.stackplot(hour, [...])`~~

* make a couple load curves of blockgroups that look different. 

* run TNC simuluation -> load_curves
    - only change is public/home l2 logic

* check on the charging and making sure we arent missing charging events. Try only adding chargin times when they start and not making 24 windows. 


* loop the years and outputs so we can get this done as fast as possible
* ~~get matt to figure out charging numbers~~


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

