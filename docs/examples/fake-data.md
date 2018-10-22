# Generating Fake Data 

To work with MDS data, you may in fact, need to generate fake data for a variety of purposes. 

To facililate that, *MDS Provider* has a `mds.fake` module that assists in the development of fake data. 

## Examples

To generate data, you'll need to setup a `ProviderDataGenerator`. 
```python 
from mds.fake.provider import ProviderDataGenerator


gen = ProviderDataGenerator(
        boundary=boundary,
        speed=speed,
        vehicle_types=args.vehicle_types,
        propulsion_types=args.propulsion_types)

```

The above code creates a generator. To use that generator to make trips. The following example generates trips and status changes for 1 day, as definied by the variables. 

```python 

day_status_changes, day_trips = \
gen.service_day(devices, date, hour_open, hour_closed, inactivity)
```

There are a number of options to help generate fake data. For example of the options in use, see the [mds-provider-services](https://github.com/CityofSantaMonica/mds-provider-services/blob/master/fake/main.py) repo. 
