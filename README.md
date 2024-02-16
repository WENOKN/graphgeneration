# Graph Generator

## About

Just some hackish code to conver the parquet files like int he input 
directory into RDF.

For now this is just done by populating the template.json and then converting
that to RDF (nt) and loading that back into the dataframe.

This can either be:

- saved out as a new parqet file
- loaded into a pyoxigraph tgriplestore (memory issues)

## TODO

- [ ] sync with tests at [colab notebook](https://colab.research.google.com/drive/1BYJhx35WXio8qkgHm1pZ2_RNn4SgE76N#scrollTo=teNK6oSLKl7i)
- [ ] Bring in my Dask code to parallelize the process
- [ ] loop on parquet files and load to triplestore
- [ ] put in the S2 grid cell into the RDF (level 30?) 
- [ ] link the above S2 to KWG level 13 resources
- [ ] HUCs and cenus ID   (how to put these in)

## Questions

Is it valid for the values to be doubles in the lat long area

```n-triples
_:c14n5 <https://schema.org/elevation> "8.809310913085938E0"^^<http://www.w3.org/2001/XMLSchema#double> .
_:c14n5 <https://schema.org/latitude> "3.868890300000552E1"^^<http://www.w3.org/2001/XMLSchema#double> .
_:c14n5 <https://schema.org/longitude> "-8.359915550000338E1"^^<http://www.w3.org/2001/XMLSchema#double> .
_:c14n6 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <https://schema.org/PropertyValue> .
```

## Add in S2 and link to KWG

simple with only lat long to worry about  (elevation?)

> Note geometries like polygon can be done too, but a bit more complex and not needed for this

```python
def get_cell_id( latitude: float, longitude: float, level: int ) -> int:
    return CellId.from_lat_lng(LatLng(latitude, longitude)).parent(level).id()
```

```python 
latitude, longitude = (34.4208, -86.915)

print(get_cell_id(latitude,longitude,13))
```


## Commands

Example object path: 

```bash
s3://backup.udp-data-urmi/STATEFP=56/COUNTYFP=045/data.parquet
```

Example to build for Ohio
```Bash
python tab2rdf.py --source s3://s3.amazonaws.com/backup.udp-data-urmi/STATEFP=39 --outputdir /mnt/wdb/scratch/parquet
```


### Notes for Duck Loader

Convert one county to a graph. Note that now you provide a path to a file, not a directory for output to make this
just better.  
```bash
 python duckLoader.py --sourcefile  /mnt/wdb/scratch/parquet/fip_39039.parquet --outputfile /mnt/wdb/scratch/graphs/fip39.nq
```

You can also use --sourcedir and provide a directory to convert all the .parquet files in that directory into 
a file.  For building out a whole state for example.
