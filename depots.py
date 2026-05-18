### https://www.berlinstadtservice.de/xinh/Bus_Betriebshof_Berlin.html
### Add depots
stoppoints = pd.concat([stoppoints, pd.DataFrame([{
    'ID': 900000000001,
    'Code': 'DEPOT',
    'Name': 'Betriebshof Weißensee',
    'Lat': 52.545699,
    'Lon': 13.468622,
    'VehCapacityForCharging': 120
}])], axis=0)

stoppoints = pd.concat([stoppoints, pd.DataFrame([{
    'ID': 900000000002,
    'Code': 'DEPOT',
    'Name': 'Betriebshof Lichtenberg',
    'Lat': 52.519746,
    'Lon': 13.499957,
    'VehCapacityForCharging': 50
}])], axis=0)

stoppoints = pd.concat([stoppoints, pd.DataFrame([{
    'ID': 900000000003,
    'Code': 'DEPOT',
    'Name': 'Betriebshof Wedding',
    'Lat': 52.552370,
    'Lon': 13.349366,
    'VehCapacityForCharging': 120
}])], axis=0)

stoppoints = pd.concat([stoppoints, pd.DataFrame([{
    'ID': 900000000004,
    'Code': 'DEPOT',
    'Name': 'Betriebshof Spandau',
    'Lat': 52.517266,
    'Lon': 13.183191,
    'VehCapacityForCharging': 120
}])], axis=0)

stoppoints = pd.concat([stoppoints, pd.DataFrame([{
    'ID': 900000000005,
    'Code': 'DEPOT ',
    'Name': 'Betriebshof Neukölln',
    'Lat': 52.453568,
    'Lon': 13.422036,
    'VehCapacityForCharging': 120

}])], axis=0)

stoppoints = pd.concat([stoppoints, pd.DataFrame([{
    'ID': 900000000006,
    'Code': 'DEPOT',
    'Name': 'Betriebshof Wilmersdorf',
    'Lat': 52.494360,
    'Lon': 13.301960,
    'VehCapacityForCharging': 120

}])], axis=0)