## Bug ParseCorpus ##

* *indexTerm* tries to delte *part-00000* but in CDH4.2.0, it is *part-r-00000* 

* *configure* of *indexDocument*
```
if (path.getName().startsWith(TERM)) {
    Preconditions.checkArgument(termIndex == null,
    	"Term index was initialized already...");
    termIndex = ParseCorpus.importParameter(sequenceFileReader);
}
if (path.getName().startsWith(TITLE)) {
    Preconditions.checkArgument(titleIndex == null,
    	"Title index was initialized already...");
    titleIndex = ParseCorpus.importParameter(sequenceFileReader);
} else {
    throw new IllegalArgumentException("Unexpected file in distributed cache: "
    	+ path.getName());
} 
```
Should be `if ... else if ... else`
	
## Coding flow Note ##


### ParseCorpus ###
* tokenize corpus: count document-frequency and term-frequency of each term (word). Also, write out document:(doc_title,doc_content) and title(doc_title,null).
* index titles: auto-increment indices
* index terms: reduce the dictionary size to terms (words) with document frequency within a range. Also assign auto-increment indices to the remaining terms (thus the use of 1 reducer)
	