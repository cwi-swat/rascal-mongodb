module resource::mongodb::MongoDB

@javaClass{org.rascalmpl.mongodb.Bridge}
java set[loc] listDatabases(loc server);

@javaClass{org.rascalmpl.mongodb.Bridge}
java set[loc] listCollections(loc database);

@javaClass{org.rascalmpl.mongodb.Bridge}
java set[value] getCollection(loc collection);