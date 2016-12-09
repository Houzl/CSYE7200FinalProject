# NCBI Taxonomy Search Engine -- CSYE 7200 Final Project

The original idea comes from Professor Hillyard. We will read the taxonomic tree from this site: https://www.ncbi.nlm.nih.gov/taxonomy and represent it using DataFrame and GraphFrame (part of Spark).

Team member: Zhilong Hou, Li Ma

The dataset: ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdmp.zip
To run the program you need download this zip file, unzip it and change "val path" in all *Spec.scala files to the real path in you system. 

The GraphBuilder repository includes all the approaches we have tried to accomplish functions for searching Taxonomy tree, and include all the test case.

The GraphBuilder_With_Play repository includes the integration of our algorithm with play MVC Framework,
All the search result are parsed to a RESTFul-like Json response which would be used to draw hierarchy graph in the Front End.

The Web UI-D3 repository includes all the html pages for displaying three fundamental search result: Getchild, GetPathToRoot and Get Siblings. By implementing D3.js which renders the JSON response dynamically.