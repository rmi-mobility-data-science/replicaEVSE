# TODO:

## LDV
* ~~Precalc dwell time into main dataset.~~

* ~~check that the sampling logic is working~~
* fix sampling logic: each year the same length still. 
must be in the row[year] or something. 

* keep track of unused charge through out the day

* run sim
    - now with PHEV using logic in the sharepoint file.
    - these are now in the output from the sampling code
    - check with Ben if the logic changes

* check on the charging and making sure we arent missing charging events. Try only adding charging times when they start and not making 24h windows. 

* gut check: vmt vs efficiency to get total power needed. 
    - run on state wide values
    - could be in the merge which we don't use anymore?

* Update this: Summary statistic X% of charging takes place at home, Y% charges public Z% charges at work. Use charge_id and wrap all work together. 

* ask emily for plot comparison ideas

* make a couple load curves of blockgroups that look different.

* implement new logic:
    - precalc stop duration
    - See below

* Fill out the assumptions with code

* there is a bug in the replica data where a trip will end after the next one is supposed to have started. this creates negative stop_durations. 

---


# Notes from dave
1. need to check that a vehicle doesn't meet its daily driving needs
    * in future: assume this demand would be met with Public DCFC at the last Public spot


### Changes to model based on dwell time
    if public:
        if dwell time < 60 mins, 
            90% DCFC bimonial
        else:
            10% L2

    

---

## TNCs

* Look up summary stats and distributiions for TNCs
    - stop duration
    - distance per trip 
    - find shortest dwell time

* run TNC simuluation -> load_curves
    - only change is public/home l2 logic

* loop the years and outputs so we can get this done as fast as possible
* ~~get matt to figure out charging numbers~~


---

# Notes as I explore the data

### Commercial
* MHDV thursday vmt is ~16m miles
* Matt M. has taken over the Commercial and implemented his own functionality
* commercial travel in the trips tables have a person_id, however these person_ids are not in the population table. This is fine for PRIVATE_AUTO but will require modification of the code for COMMERCIAL trips. 
* for the northwest region, we only have information on whether
the vehicle type is Heavy or Medium duty trucks. this may not be true in other regions.
* there is only 1 person per trip in the commercial trips data
* None of the commercial drivers are in the population data
* There is no `stop_duration` for trucks

### lengths of the full northwest region data files in millions
these are useful for catching bugs
* pop_len = 58.222322
* trips_len = 159.624453 
* other_len = 49.674863
* pop_len = 14.889896
* trips_into_wa_len = 51.727268
* private auto trips into wa = 27.12446
* merged_len = 51.727268 
* unique persons who travelled to wa = 8.538399



### Counties
* only LDV have county information from the population table. Commercial Does not. Using destinatino blockgroup to get counties associated with each person_id (trip for commercial)

### Charger availability
* need to include every charge but set the charging value to 0. 

