# replicaEVSE
Using [replica](https://www.replicahq.com) [trips](https://documentation.replicahq.com/docs/auto-tnc-trips) and population data to model EV use and EVSE needs. 

## Assumptions for the model

1. One person traveling with `mode == PRIVATE_AUTO` is a single vehicle. 
This is complicated with the fact that we randomly sample people from counties with either SFH or MFH. 
So the same person might not have the same car each year but they do have the same vehicle from weekdays to weekends. 

1. We assume travel behavior will be the same as people switch from ICE to EV

1. Persons who live in Mobile housing don't have EVs. This might change soon.

1. For the WA-TES project, we consider everyone whos desitination ends in WA. We also use destination county as a proxy for their home county for non LDV segments. LDVs have a home county since they are in the population table. 

### WA stock rollover sampling

1. If there are more people in a county, vehicle type, domicile, powertrian, combination: sample the remainder from the full county population. This should randomize who has two 
vehicles.