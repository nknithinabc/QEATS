/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.repositoryservices;

import ch.hsr.geohash.GeoHash;
import com.crio.qeats.configs.RedisConfiguration;
import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.globals.GlobalConstants;
import com.crio.qeats.models.ItemEntity;
import com.crio.qeats.models.MenuEntity;
import com.crio.qeats.models.RestaurantEntity;
import com.crio.qeats.repositories.ItemRepository;
import com.crio.qeats.repositories.MenuRepository;
import com.crio.qeats.repositories.RestaurantRepository;

import com.crio.qeats.utils.GeoLocation;
import com.crio.qeats.utils.GeoUtils;
import com.fasterxml.jackson.core.JsonParseException;

import com.fasterxml.jackson.core.JsonProcessingException;

import com.fasterxml.jackson.core.type.TypeReference;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.modelmapper.ModelMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;




// @Primary
// public class RestaurantRepositoryServiceImpl implements RestaurantRepositoryService {


//   @Autowired
//   private MongoTemplate mongoTemplate;

//   @Autowired
//   private Provider<ModelMappe@Servicer> modelMapperProvider;

//   @Autowired
//   private RestaurantRepository restaurantRepository;


@Service
@Primary
@Component
public class RestaurantRepositoryServiceImpl implements RestaurantRepositoryService {


  @Autowired
  private RedisConfiguration redisConfiguration;

  @Autowired
  private MongoTemplate mongoTemplate;

  @Autowired
  private Provider<ModelMapper> modelMapperProvider;

  @Autowired
  private RestaurantRepository restaurantRepository;

  private boolean isOpenNow(LocalTime time, RestaurantEntity res) {
    LocalTime openingTime = LocalTime.parse(res.getOpensAt());
    LocalTime closingTime = LocalTime.parse(res.getClosesAt());

    return time.isAfter(openingTime) && time.isBefore(closingTime);
  }

  // TODO: CRIO_TASK_MODULE_NOSQL
  // Objectives:
  // 1. Implement findAllRestaurantsCloseby.
  // 2. Remember to keep the precision of GeoHash in mind while using it as a key.
  // Check RestaurantRepositoryService.java file for the interface contract.
  public List<Restaurant> findAllRestaurantsCloseBy(Double latitude,
      Double longitude, LocalTime currentTime, Double servingRadiusInKms) {

    List<Restaurant> restaurants = new ArrayList<>();
    ObjectMapper objectMapper = new ObjectMapper();

    String hash = "";
    String listOfRestaurants = ""; 

    redisConfiguration.initCache();

    GeoHash geohash = GeoHash.withCharacterPrecision(latitude, longitude,7);
    hash = geohash.toBase32();

    Jedis jedis = redisConfiguration.getJedisPool().getResource();

    if (redisConfiguration.isCacheAvailable() && jedis.exists(hash)) {
      
      listOfRestaurants = jedis.get(hash);
      try {
        restaurants = objectMapper.readValue(listOfRestaurants, 
        new TypeReference<List<Restaurant>>() {
          });
      } catch (Exception e) {
        e.printStackTrace();
      }

      return restaurants;

    } else {

      List<RestaurantEntity> rests = restaurantRepository.findAll();
      // List<RestaurantEntity> rests = mongoTemplate.findAll(RestaurantEntity.class);

      for (RestaurantEntity restaurantEntity : rests) {
        if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime, 
            latitude, longitude, servingRadiusInKms)) {
          Restaurant restaurant = modelMapperProvider.get().map(restaurantEntity, Restaurant.class);

          restaurants.add(restaurant);
        }
      }

      geohash = GeoHash.withCharacterPrecision(latitude, longitude, 7);
      hash = geohash.toBase32();

      try {
        jedis.set(hash, objectMapper.writeValueAsString(restaurants));
      } catch (Exception e) {
        e.printStackTrace();
      }

      // TODO: CRIO_TASK_MODULE_REDIS
      // We want to use cache to speed things up. Write methods that perform the same
      // functionality,
      // but using the cache if it is present and reachable.
      // Remember, you must ensure that if cache is not present, the queries are
      // directed at the
      // database instead.
      return restaurants;
    }
  }


  
  

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants whose names have an exact or partial match with the search query.
  @Override
  public List<Restaurant> findRestaurantsByName(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
      
    List<RestaurantEntity> restaurantEntities = new ArrayList<>();
    List<Restaurant> restaurants = new ArrayList<>();


    restaurantEntities = restaurantRepository.findRestaurantsByNameExact(searchString).get();

    for (RestaurantEntity restaurantEntity : restaurantEntities) {
      if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime, 
          latitude, longitude, servingRadiusInKms)) {

        Restaurant restaurant = modelMapperProvider.get().map(restaurantEntity, Restaurant.class);

        if (restaurant.getName().contains(searchString)) {

          restaurants.add(restaurant);
        }
      }
    }

    return restaurants;
  }
  
  @Override
  @Async
  public CompletableFuture<List<Restaurant>> findRestaurantsByNameAsync(Double latitude, 
         Double longitude,String searchString, LocalTime currentTime, Double servingRadiusInKms) {
      
    List<RestaurantEntity> restaurantEntities = new ArrayList<>();
    List<Restaurant> restaurants = new ArrayList<>();

    restaurantEntities = restaurantRepository.findRestaurantsByNameExact(searchString).get();

    for (RestaurantEntity restaurantEntity : restaurantEntities) {
      if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime, 
          latitude, longitude, servingRadiusInKms)) {

        Restaurant restaurant = modelMapperProvider.get().map(restaurantEntity, Restaurant.class);

        if (restaurant.getName().contains(searchString)) {

          restaurants.add(restaurant);
        }
      }
    }

    return CompletableFuture.completedFuture(restaurants);
  }


  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants whose attributes (cuisines) intersect with the search query.
  @Override
  public List<Restaurant> findRestaurantsByAttributes(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
      
    List<RestaurantEntity> restaurantEntities = new ArrayList<>();
    List<Restaurant> restaurants = new ArrayList<>();


    restaurantEntities = restaurantRepository.findAll();

    for (RestaurantEntity restaurantEntity : restaurantEntities) {

      if (isRestaurantCloseByAndOpen(restaurantEntity,
           currentTime, latitude, longitude, servingRadiusInKms)) {

        Restaurant restaurant = modelMapperProvider.get().map(restaurantEntity, Restaurant.class);

        if (restaurant.getAttributes().contains(searchString)) {
          restaurants.add(restaurant);
        }
      }

    }
    
    return restaurants;
  }

  @Override
  @Async
  public CompletableFuture<List<Restaurant>> findRestaurantsByAttributesAsync(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
      
    List<RestaurantEntity> restaurantEntities = new ArrayList<>();
    List<Restaurant> restaurants = new ArrayList<>();


    restaurantEntities = restaurantRepository.findAll();

    for (RestaurantEntity restaurantEntity : restaurantEntities) {

      if (isRestaurantCloseByAndOpen(restaurantEntity,
           currentTime, latitude, longitude, servingRadiusInKms)) {

        Restaurant restaurant = modelMapperProvider.get().map(restaurantEntity, Restaurant.class);

        if (restaurant.getAttributes().contains(searchString)) {
          restaurants.add(restaurant);
        }
      }

    }
    
    return CompletableFuture.completedFuture(restaurants);
  }





  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants which serve food items whose names form a complete or partial match
  // with the search query.

  @Override
  public List<Restaurant> findRestaurantsByItemName(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {

    BasicQuery query = new BasicQuery("{'items.name': {$regex: /" + searchString + "/i}}");
    List<MenuEntity> menus = mongoTemplate.find(query, MenuEntity.class, "menus");

    List<RestaurantEntity> restaurantEntities = new ArrayList<>();

    for (MenuEntity menu : menus) {
      String restaurantId = menu.getRestaurantId();
      BasicQuery restaurantQuery = new BasicQuery("{restaurantId:" + restaurantId + "}");
      restaurantEntities.add(mongoTemplate.findOne(
          restaurantQuery, RestaurantEntity.class, "restaurantEntities"));
    }

    List<Restaurant> restaurants = new ArrayList<>();

    for (RestaurantEntity restaurantEntity : restaurantEntities) {
      if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime, 
          latitude, longitude, servingRadiusInKms)) {
        Restaurant restaurant = modelMapperProvider.get().map(restaurantEntity, Restaurant.class);
        restaurants.add(restaurant);
      }
    }

    return restaurants;
  }
  
  @Override
  @Async
  public CompletableFuture<List<Restaurant>> findRestaurantsByItemNameAsync(
      Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {

    BasicQuery query = new BasicQuery("{'items.name': {$regex: /" + searchString + "/i}}");
    List<MenuEntity> menus = mongoTemplate.find(query, MenuEntity.class, "menus");

    List<RestaurantEntity> restaurantEntities = new ArrayList<>();

    for (MenuEntity menu : menus) {
      String restaurantId = menu.getRestaurantId();
      BasicQuery restaurantQuery = new BasicQuery("{restaurantId:" + restaurantId + "}");
      restaurantEntities.add(mongoTemplate.findOne(
          restaurantQuery, RestaurantEntity.class, "restaurantEntities"));
    }

    List<Restaurant> restaurants = new ArrayList<>();

    for (RestaurantEntity restaurantEntity : restaurantEntities) {
      if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime, 
          latitude, longitude, servingRadiusInKms)) {
        Restaurant restaurant = modelMapperProvider.get().map(restaurantEntity, Restaurant.class);
        restaurants.add(restaurant);
      }
    }

    return CompletableFuture.completedFuture(restaurants);
  }

  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Objective:
  // Find restaurants which serve food items whose attributes intersect with the search query.
  @Override
  public List<Restaurant> findRestaurantsByItemAttributes(Double latitude, Double longitude,
      String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    
    BasicQuery query = new BasicQuery("{'items.attributes': {$regex: /" + searchString + "/i}}");
    List<MenuEntity> menus = mongoTemplate.find(query, MenuEntity.class, "menus");

    List<RestaurantEntity> restaurantEntities = new ArrayList<>();

    for (MenuEntity menu : menus) {
      String restaurantId = menu.getRestaurantId();
      BasicQuery restaurantQuery = new BasicQuery("{restaurantId:" + restaurantId + "}");
      restaurantEntities.add(mongoTemplate.findOne(restaurantQuery, 
          RestaurantEntity.class, "restaurantEntities"));
    }

    List<Restaurant> restaurants = new ArrayList<>();

    for (RestaurantEntity restaurantEntity : restaurantEntities) {
      if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime, 
          latitude, longitude, servingRadiusInKms)) {
        Restaurant restaurant = modelMapperProvider.get().map(restaurantEntity, Restaurant.class);
        restaurants.add(restaurant);
      }
    }

    return restaurants;
        
  }

  @Override
  @Async
  public CompletableFuture<List<Restaurant>> findRestaurantsByItemAttributesAsync(Double latitude, 
      Double longitude,String searchString, LocalTime currentTime, Double servingRadiusInKms) {
    
    BasicQuery query = new BasicQuery("{'items.attributes': {$regex: /" + searchString + "/i}}");
    List<MenuEntity> menus = mongoTemplate.find(query, MenuEntity.class, "menus");

    List<RestaurantEntity> restaurantEntities = new ArrayList<>();

    for (MenuEntity menu : menus) {
      String restaurantId = menu.getRestaurantId();
      BasicQuery restaurantQuery = new BasicQuery("{restaurantId:" + restaurantId + "}");
      restaurantEntities.add(mongoTemplate.findOne(restaurantQuery, 
          RestaurantEntity.class, "restaurantEntities"));
    }

    List<Restaurant> restaurants = new ArrayList<>();

    for (RestaurantEntity restaurantEntity : restaurantEntities) {
      if (isRestaurantCloseByAndOpen(restaurantEntity, currentTime, 
          latitude, longitude, servingRadiusInKms)) {
        Restaurant restaurant = modelMapperProvider.get().map(restaurantEntity, Restaurant.class);
        restaurants.add(restaurant);
      }
    }

    return CompletableFuture.completedFuture(restaurants);
        
  }


  /**
   * Utility method to check if a restaurant is within the serving radius at a given time.
   * @return boolean True if restaurant falls within serving radius and is open, false otherwise
   */
  private boolean isRestaurantCloseByAndOpen(RestaurantEntity restaurantEntity,
      LocalTime currentTime, Double latitude, Double longitude, Double servingRadiusInKms) {
    if (isOpenNow(currentTime, restaurantEntity)) {
      return GeoUtils.findDistanceInKm(latitude, longitude,
          restaurantEntity.getLatitude(), restaurantEntity.getLongitude())
          < servingRadiusInKms;
    }

    return false;
  }



}

