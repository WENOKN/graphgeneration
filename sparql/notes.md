# Notes

## KGW ontology

https://stko-kwg.geog.ucsb.edu/lod/ontology

```sparql

select * where { 
     SERVICE <https://stko-kwg.geog.ucsb.edu/workbench/repositories/KWG> {
            select * where {
	        ?s kwg-ont:hasID ?o .
        } limit 100
	} 
}
```


select * where {
		?s geo:hasGeometry kwgr:geometry.polygon.9854746815075713024 .
		}
LIMIT 100


* Feature_id is the catchment, might want IRI for it
* GEOID   geohash????
* Source_ID is really a derived from, could be (along with others)
used to make a description
* Is there an IRI for s2 level X??  (KWG has one)

http://stko-kwg.geog.ucsb.edu/lod/resource/s2.level13.9866369615053979648

https://stko-kwg.geog.ucsb.edu/lod/ontology#S2Cell





