/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.crud;

/**
 *
 * @author Noe Guevara
 */
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class CRUDMongoDB {
    public static void main(String[]args) {
        
        MongoClient mongo = crearConexion();
        
        // SI NO EXISTE LA BASE DE DATOS LA CREAMOS
        if(mongo != null) {
            DB db = mongo.getDB("Pruebas");
            
            //System.out.println("BASE DE DATOS CREADA");
            // CREA UNA COLECCION(TABLA) SI NO EXISTE E
            // INSERTA EL DOCUMENTO(REGISTRO) A LA COLECCION
            //insertarUsuario(db, "usuarios", "Sergio", "Mexico");
            //insertarUsuario(db, "usuarios", "Laura", "Colombia");
            //insertarUsuario(db, "usuarios", "Franco", "Chile");
            
            //mostrarColeccion(db, "usuarios");
            //buscarPorNombre(db, "usuarios", "Sergio");
            
            //System.out.println("ANTES DEL UPDATE");
            //mostrarColeccion(db, "usuarios");
            //actualizarDocumento(db, "usuarios", "Sergio");
            //System.out.println("DESPUES DEL UPDATE");
            //mostrarColeccion(db, "usuarios");
            
            //System.out.println("ANTES DEL DELETE");
            //mostrarColeccion(db, "usuarios");
            //borrarDocumento(db, "usuarios", "Colombia");
            //System.out.println("DESPUES DEL DELETE");
            //mostrarColeccion(db, "usuarios");
        }
        
    }
    
    // METODO PARA CREAR LA CONEXION A MONGODB
    public static MongoClient crearConexion() {
        System.out.println("PRUEBA CONEXION MONGODB");
        
        MongoClient mongo = null;
        
        mongo = new MongoClient("localhost", 27017);
        
        return mongo;
    }
    
    // METODO PARA INSERTAR UN DOCUMENTO (REGISTRO)
    public static void insertarUsuario(DB db, String coleccion, String nombre, String pais) {
        DBCollection colec = db.getCollection(coleccion);
        
        // CREA EL DOCUMENTO(REGISTRO) E INSERTA LA INFORMACION RECIBIDA
        BasicDBObject documento = new BasicDBObject();
        documento.put("nombre", nombre);
        documento.put("pais", pais);
        
        colec.insert(documento);
        
    }
    
    // MUESTRA TODOS LOS DOCUMENTOS DE LA COLECCION USUARIOS
    public static void mostrarColeccion(DB db, String coleccion) {
        DBCollection colec = db.getCollection(coleccion);
        
        DBCursor cursor = colec.find();
        
        while(cursor.hasNext()) {
            System.out.println("* "+ cursor.next().get("nombre") + " - " + cursor.curr().get("pais"));
        }
    }
    
    // MUESTRA TODOS LOS DOCUMENTOS DE LA COLECCION USUARIOS QUE COINCIDAN CON EL NOMBRE
    public static void buscarPorNombre(DB db, String coleccion, String nombre) {
        DBCollection colect = db.getCollection(coleccion);
        
        // CREAMOS LA CONSULTA CON EL CAMPO NOMBRE
        BasicDBObject consulta = new BasicDBObject();
        consulta.put("nombre", nombre);
        
        // BUSCA Y MUESTRA TODOS LOS DOCUMENTOS QUE COINCIDAN CON LA CONSULTA
        DBCursor cursor = colect.find(consulta);
        while(cursor.hasNext()) {
            System.out.println("-- " + cursor.next().get("nombre") + " - " + cursor.curr().get("pais"));
        }
    }
    
    // METODO PARA ACTUALIZAR UN DOCUMENTO (REGISTRO)
    public static void actualizarDocumento(DB db, String coleccion, String nombre) {
        DBCollection colec = db.getCollection(coleccion);
        
        // SENTENCIA CON LA INFORMACION A REMPLAZAR
        BasicDBObject actualizarPais = new BasicDBObject();
        actualizarPais.append("$set", new BasicDBObject().append("pais", "Peru"));
        
        // BUSCA EL DOCUMENTO EN LA COLECCION
        BasicDBObject buscarPorNombre = new BasicDBObject();
        buscarPorNombre.append("nombre", nombre);
        
        // REALIZA EL UPDATE
        colec.updateMulti(buscarPorNombre, actualizarPais);
    }
    
    // METODO PARA ELIMINAR UN DOCUMENTO (REGISTRO)
    public static void borrarDocumento(DB db, String coleccion, String nombre) {
        DBCollection colec = db.getCollection(coleccion);
        
        colec.remove(new BasicDBObject().append("pais", nombre));
    }
}