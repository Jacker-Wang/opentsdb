// This file is part of OpenTSDB.
// Copyright (C) 2010  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.exc.UnrecognizedPropertyException;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class will help with all JSON related behavior such as parsing or
 * creating JSON strings. It's to help cut down on code repetition such as
 * catching Jackson errors, etc.
 */
final class JsonHelper {
  /** Logging object for this class */
  private static final Logger LOG = LoggerFactory.getLogger(JsonRpcError.class);

  /**
   * Jackson serializer This is public so that other JSON dependencies can use
   * it
   */
  public static ObjectMapper JsonMapper = new ObjectMapper();

  /**
   * an object that the serializer will convert into a string OR the
   * deserializer will use for type conversion
   */
  private Object object = null;

  /** Holds any error messages generated by the mapper */
  private String error = "";

  /**
   * Default constructor
   */
  public JsonHelper() {
  }

  /**
   * Constructor that assigns the local object with whatever you pass as a
   * parameter
   * @param o An object to serialize or the type to deserialize
   */
  public JsonHelper(Object o) {
    object = o;
  }

  /**
   * Attempts to parse the provided JSON string using the class that was passed
   * to the constructor. NOTE: The an object must have been provided via the
   * constructor or this method will log an error and return false
   * @param json A JSON formatted string to deserialize
   * @return True if deserialization was successful (no errors encounterd) or
   *         false if there was an error. Check the error string via
   *         {@link getError} to find out what went wrong
   */
  public final boolean parseObject(final String json) {
    if (object == null) {
      error = "TSD Error: The object was null";
      return false;
    }

    // try parsing
    try {
      error = "";
      object = JsonMapper.readValue(json, object.getClass());
      return true;

    } catch (UnrecognizedPropertyException e) {
      error = e.getMessage();
      LOG.error(error);
      LOG.trace(json);
    } catch (JsonParseException e) {
      error = e.getMessage();
      LOG.error(error);
      LOG.trace(json);
    } catch (JsonMappingException e) {
      error = e.getMessage();
      LOG.error(error);
      LOG.trace(json);
    } catch (IOException e) {
      error = e.getMessage();
      LOG.error(error);
      LOG.trace(json);
    } catch (Exception e) {
      error = e.getMessage();
      LOG.error(error);
      LOG.trace(json);
    }
    return false;
  }

  /**
   * Attempts to parse the provided JSON string using a provided type reference.
   * @param json A JSON formatted string to deserialize
   * @param type The type of object to load into
   * @return True if deserialization was successful (no errors encounterd) or
   *         false if there was an error. Check the error string via
   *         {@link getError} to find out what went wrong
   */
  public final boolean parseObject(final String json,
      final TypeReference<?> type) {
    LOG.info("Made it here in the helper");
    // try parsing
    try {
      error = "";
      object = JsonMapper.readValue(json, type);
      return true;

    } catch (UnrecognizedPropertyException e) {
      error = e.getMessage();
      LOG.error(error);
      LOG.trace(json);
    } catch (JsonParseException e) {
      error = e.getMessage();
      LOG.error(error);
      LOG.trace(json);
    } catch (JsonMappingException e) {
      error = e.getMessage();
      LOG.error(error);
      LOG.trace(json);
    } catch (IOException e) {
      error = e.getMessage();
      LOG.error(error);
      LOG.trace(json);
    } catch (Exception e) {
      error = e.getMessage();
      LOG.error(error);
      LOG.trace(json);
    }
    return false;
  }

  /**
   * Returns a JSON formatted string of the Object provided in the constructor.
   * @return A JSON formatted string if successful, an empty string if there was
   *         an error. Check {@link getError} to find out what went wrong
   */
  public final String getJsonString() {
    if (object == null) {
      error = "TSD Error: The object was null";
      return "";
    }

    try {
      error = "";
      return JsonMapper.writeValueAsString(object);
    } catch (JsonGenerationException e) {
      error = e.getMessage();
      LOG.error(error);
    } catch (JsonMappingException e) {
      error = e.getMessage();
      LOG.error(error);
    } catch (IOException e) {
      error = e.getMessage();
      LOG.error(error);
    }
    return "";
  }

  /**
   * Returns a JSON formatted string of the Object provided in the constructor.
   * The JSON string is enclosed in a Javascript style function call for
   * cross-site scripting purposes.
   * @param function
   * @return A Javascript formatted string if successful, an empty string if
   *         there was an error. Check {@link getError} to find out what went
   *         wrong
   */
  public final String getJsonPString(final String function) {
    error = "";
    String json = getJsonString();
    if (json.isEmpty())
      return "";
    return (function.length() > 0 ? function : "parseTSDResponse") + "(" + json
        + ");";
  }

  /**
   * Checks the HttpQuery to see if the json=functionName exists and if
   * so then it returns the function name. If not, it returns an empty string
   * @param query The HttpQuery to parse for "json"
   * @return The function name supplied by the user or an empty string
   */
  public static final String getJsonPFunction(final HttpQuery query){
    if (query.hasQueryStringParam("json"))
      return query.getQueryStringParam("json");
    else
      return "";
  }
  
  /**
   * Checks the HttpQuery to see if the query includes "json" and the user
   * wants to get a JSON formatted response
   * @param query The HttpQuery to parse for "json"
   * @return True if the parameter was included, false if not
   */
  public static final boolean getJsonRequested(final HttpQuery query){
    return query.hasQueryStringParam("json");
  }
  
  /** Gets the error string */
  public final String getError() {
    return error;
  }

  /** Gets the object */
  public final Object getObject() {
    return object;
  }
}
