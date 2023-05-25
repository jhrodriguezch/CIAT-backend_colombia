
# CIAT-backend_colombia

Backend routines for the IDEAM tethys colombia apps.

## Description

This tool facilitates the population of databases created for the tools developed in Tethys for IDEAM. The tool requests data from the observed database hosted in HydroShare, as well as the simulated data in the GeoGloWS database. Additionally, it generates alert levels associated with different study stations.

## Getting Started

### Dependencies

* python 3.* is recommended
* Python dependences:
    re
    math
    numpy
    pandas
    requests
    datetime
    geoglows
    psycopg2
    sqlalchemy
    concurrent

### Installing

* Does not need installing

### Executing program

* Run in python environment as a script or run bash files
```
./onetimeonly.sh
./monthly.sh
./daily.sh
```

## Help

* You need run first onetimeonly.sh, follow by monthly.sh and finally, daily.sh 
* Run after the postgres data base need be built

## Authors

Contributors names and contact info

Eng. Jhonatan Chaves [@jhrodriguezch](https://github.com/jhrodriguezch)

## Version History

* 0.1
    * Initial Release

## License

This project is licensed under the MIT License.

## Acknowledgments

*
 
