package org.rascalmpl.mongodb;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringBufferInputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.imp.pdb.facts.ISet;
import org.eclipse.imp.pdb.facts.ISetWriter;
import org.eclipse.imp.pdb.facts.ISourceLocation;
import org.eclipse.imp.pdb.facts.IString;
import org.eclipse.imp.pdb.facts.IValue;
import org.eclipse.imp.pdb.facts.IValueFactory;
import org.eclipse.imp.pdb.facts.exceptions.FactTypeUseException;
import org.rascalmpl.interpreter.utils.RuntimeExceptionFactory;
import org.rascalmpl.library.util.JSonReader;
import org.rascalmpl.uri.URIUtil;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;

public class Bridge {
  private final IValueFactory vf;
  private final Map<ISourceLocation,DB> connections;

  public Bridge(IValueFactory vf) {
    this.vf = vf;
    this.connections = new HashMap<>();
  }
  
  private DB parentConnect(ISourceLocation loc) {
    String[] path = loc.getPath().split("/");
    
    if (path.length != 3) {
      throw RuntimeExceptionFactory.illegalArgument(vf.string("Expected |<scheme>://<host>/<db>/<collection>|, but got " + loc), null, null);
    }
    
    URI uri = loc.getURI();
    return connect(vf.sourceLocation(URIUtil.assumeCorrect(uri.getScheme(), uri.getAuthority(), "/"+ path[1])));
  }
  private DB connect(ISourceLocation loc) {
    try {
      DB db = connections.get(loc);
      
      if (db == null) {
        Mongo client;  
        URI uri = loc.getURI();
        
        if (uri.getPort() != -1) {
          client = new Mongo(new ServerAddress(uri.getHost(), uri.getPort()));
        }
        else {
          client = new Mongo(new ServerAddress(uri.getHost()));
        }

        String path = loc.getPath();
        if (path.startsWith("/")) {
          path = path.substring(1);
        }
        
        db = client.getDB(path);
        
        connections.put(loc, db);
      }
      
      return db;
    } catch (UnknownHostException e) {
      throw RuntimeExceptionFactory.illegalArgument(vf.string(e.getMessage()), null, null);
    } catch (UnsupportedOperationException e) {
      throw RuntimeExceptionFactory.illegalArgument(vf.string(e.getMessage()), null, null);
    }
  }
  
  public void authenticate(ISourceLocation loc, IString user, IString password) {
    connect(loc).authenticate(user.getValue(), password.getValue().toCharArray());
  }
  
  public void clear(ISourceLocation loc) {
    connections.remove(loc);
  }
  
  public ISet listDatabases(ISourceLocation loc) {
    try {
      Mongo client;  
      if (loc.getURI().getPort() != -1) {
        client = new Mongo(new ServerAddress(loc.getAuthority(), loc.getURI().getPort()));
      }
      else {
        client = new Mongo(new ServerAddress(loc.getAuthority()));
      }
      
      ISetWriter set = vf.setWriter();

      for (String s : client.getDatabaseNames()) {
        set.insert(vf.sourceLocation(URIUtil.getChildURI(loc.getURI(), s)));
      }
      
      return set.done();
    } catch (UnknownHostException e) {
      throw RuntimeExceptionFactory.illegalArgument(vf.string(e.getMessage()), null, null);
    } catch (UnsupportedOperationException e) {
      throw RuntimeExceptionFactory.illegalArgument(vf.string(e.getMessage()), null, null);
    }
  }
  
  public ISet listCollections(ISourceLocation loc) {
    ISetWriter set = vf.setWriter();

    for (String s : connect(loc).getCollectionNames()) {
      set.insert(vf.sourceLocation(URIUtil.getChildURI(loc.getURI(), s)));
    }
    
    return set.done();
  }

  public ISet getCollection(ISourceLocation loc) {
    DBCollection collection = parentConnect(loc).getCollection(URIUtil.getURIName(loc.getURI()));
    ISetWriter set = vf.setWriter();

    try(DBCursor cursor = collection.find()) {
      for (DBObject object : cursor) {
        set.insert(object2value(object));
      }
    }
    
    return set.done();
  }

  private IValue object2value(DBObject object) {
    try {
      return new JSonReader().read(vf, new ByteArrayInputStream(object.toString().getBytes()));
    } catch (FactTypeUseException | IOException e) {
      throw RuntimeExceptionFactory.io(vf.string(e.getMessage()), null, null);
    }
  }
  
  
}
