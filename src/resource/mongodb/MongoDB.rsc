module resource::mongodb::MongoDB

@doc{
Synopsis: lists all the databases on a mongodb server

Description:

Use with `|http://localhost|`
}
@javaClass{org.rascalmpl.mongodb.Bridge}
java set[loc] listDatabases(loc server);

@doc{
Synopsis: lists all the collections in a mongodb database

Description:

Use with `|http://localhost/<database>|`
}
@javaClass{org.rascalmpl.mongodb.Bridge}
java set[loc] listCollections(loc database);

@doc{
Synopsis: get the content of a  collection in a mongodb database

Description:

Use with `|http://localhost/<database>/<collection>|`
}
@javaClass{org.rascalmpl.mongodb.Bridge}
java set[value] getCollection(loc collection);