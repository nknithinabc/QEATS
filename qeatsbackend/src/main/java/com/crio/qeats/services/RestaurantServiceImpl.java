
/*
 *
 *  * Copyright (c) Crio.Do 2019. All rights reserved
 *
 */

package com.crio.qeats.services;

import com.crio.qeats.dto.Restaurant;
import com.crio.qeats.exchanges.GetRestaurantsRequest;
import com.crio.qeats.exchanges.GetRestaurantsResponse;
import com.crio.qeats.repositoryservices.RestaurantRepositoryService;
import com.crio.qeats.repositoryservices.RestaurantRepositoryServiceImpl;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Log4j2
public class RestaurantServiceImpl implements RestaurantService {

  private final Double peakHoursServingRadiusInKms = 3.0;
  private final Double normalHoursServingRadiusInKms = 5.0;
  
  @Autowired
  private RestaurantRepositoryService restaurantRepositoryService;

  // @Autowired
  //private RestaurantRepositoryServiceImpl restaurantRepositoryServiceImpl;



  // TODO: CRIO_TASK_MODULE_RESTAURANTSAPI - Implement findAllRestaurantsCloseby.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findAllRestaurantsCloseBy(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {
    
    // if (getRestaurantsRequest.getSearchFor().equals("")) {
    //   GetRestaurantsResponse restaurantsResponse = 
    //       new GetRestaurantsResponse(new ArrayList<Restaurant>());
    //   return restaurantsResponse;
    // } else {
    //   return findRestaurantsBySearchQuery(getRestaurantsRequest, currentTime);
    // }

    List<Restaurant> restaurants = new ArrayList<>();
    if ((currentTime.isAfter(LocalTime.parse("08:00")) 
          && currentTime.isBefore(LocalTime.parse("10:00"))) 
          || (currentTime.isAfter(LocalTime.parse("13:00")) 
          && currentTime.isBefore(LocalTime.parse("14:00"))) 
          || (currentTime.isAfter(LocalTime.parse("19:00")) 
          && currentTime.isBefore(LocalTime.parse("21:00")))
          || currentTime.equals(LocalTime.parse("08:00"))
          || currentTime.equals(LocalTime.parse("10:00"))
          || currentTime.equals(LocalTime.parse("13:00"))
          || currentTime.equals(LocalTime.parse("14:00"))
          || currentTime.equals(LocalTime.parse("19:00"))
          || currentTime.equals(LocalTime.parse("21:00"))
    ) {
      restaurants = restaurantRepositoryService.findAllRestaurantsCloseBy(
          getRestaurantsRequest.getLatitude(),getRestaurantsRequest.getLongitude(),
          currentTime,peakHoursServingRadiusInKms);
      
    } else {
      restaurants = restaurantRepositoryService.findAllRestaurantsCloseBy(
          getRestaurantsRequest.getLatitude(),getRestaurantsRequest.getLongitude(), 
          currentTime,normalHoursServingRadiusInKms);
    }

    GetRestaurantsResponse resp = new GetRestaurantsResponse(restaurants);
    return resp;
    
    

  }


  // TODO: CRIO_TASK_MODULE_RESTAURANTSEARCH
  // Implement findRestaurantsBySearchQuery. The request object has the search string.
  // We have to combine results from multiple sources:
  // 1. Restaurants by name (exact and inexact)
  // 2. Restaurants by cuisines (also called attributes)
  // 3. Restaurants by food items it serves
  // 4. Restaurants by food item attributes (spicy, sweet, etc)
  // Remember, a restaurant must be present only once in the resulting list.
  // Check RestaurantService.java file for the interface contract.
  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQuery(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime) {

    Double radius;
    
    if ((currentTime.isAfter(LocalTime.parse("08:00")) 
        && currentTime.isBefore(LocalTime.parse("10:00"))) 
        || (currentTime.isAfter(LocalTime.parse("13:00")) 
        && currentTime.isBefore(LocalTime.parse("14:00"))) 
        || (currentTime.isAfter(LocalTime.parse("19:00")) 
        && currentTime.isBefore(LocalTime.parse("21:00")))
        || currentTime.equals(LocalTime.parse("08:00"))
        || currentTime.equals(LocalTime.parse("10:00"))
        || currentTime.equals(LocalTime.parse("13:00"))
        || currentTime.equals(LocalTime.parse("14:00"))
        || currentTime.equals(LocalTime.parse("19:00"))
        || currentTime.equals(LocalTime.parse("21:00"))
    ) {
      radius = 3.0;
    } else {
      radius = 5.0;
    }

    if (getRestaurantsRequest.getSearchFor().equals("")) {
      GetRestaurantsResponse restaurantsResponse = 
          new GetRestaurantsResponse(new ArrayList<Restaurant>());
      return restaurantsResponse;
    }

    List<Restaurant> restaurants1 = new ArrayList<>();
    List<Restaurant> restaurants2 = new ArrayList<>();
    List<Restaurant> restaurants3 = new ArrayList<>();

    restaurants1 = restaurantRepositoryService.findRestaurantsByName(
                    getRestaurantsRequest.getLatitude(),getRestaurantsRequest.getLongitude(), 
                        getRestaurantsRequest.getSearchFor(),currentTime, radius);

    restaurants2 = restaurantRepositoryService.findRestaurantsByAttributes(
                    getRestaurantsRequest.getLatitude(),getRestaurantsRequest.getLongitude(), 
                        getRestaurantsRequest.getSearchFor(),currentTime, radius);

    restaurants3 = restaurantRepositoryService.findRestaurantsByItemName(
                    getRestaurantsRequest.getLatitude(),getRestaurantsRequest.getLongitude(),
                        getRestaurantsRequest.getSearchFor(), currentTime, radius);
    
    List<Restaurant> restaurants4 = new ArrayList<>();

    restaurants4 = restaurantRepositoryService.findRestaurantsByItemAttributes(
                    getRestaurantsRequest.getLatitude(),getRestaurantsRequest.getLongitude(),
                        getRestaurantsRequest.getSearchFor(), currentTime, radius);
    
    restaurants1.addAll(restaurants2);
    restaurants1.addAll(restaurants3);
    restaurants1.addAll(restaurants4);

    List<Restaurant> restaurants = new ArrayList<>();
    Set<Restaurant> set = new LinkedHashSet<Restaurant>(restaurants1);
    restaurants = new ArrayList<>(set);

    GetRestaurantsResponse restaurantsResponse = new GetRestaurantsResponse(restaurants);
    
    return restaurantsResponse;
  }

  

  // TODO: CRIO_TASK_MODULE_MULTITHREADING
  // Implement multi-threaded version of RestaurantSearch.
  // Implement variant of findRestaurantsBySearchQuery which is at least 1.5x time faster than
  // findRestaurantsBySearchQuery.
  @Override
  public GetRestaurantsResponse findRestaurantsBySearchQueryMt(
      GetRestaurantsRequest getRestaurantsRequest, LocalTime currentTime)
      throws InterruptedException, ExecutionException {

    Double radius;

    if ((currentTime.isAfter(LocalTime.parse("08:00")) 
        && currentTime.isBefore(LocalTime.parse("10:00")))
        || (currentTime.isAfter(LocalTime.parse("13:00")) 
        && currentTime.isBefore(LocalTime.parse("14:00")))
        || (currentTime.isAfter(LocalTime.parse("19:00")) 
        && currentTime.isBefore(LocalTime.parse("21:00")))
        || currentTime.equals(LocalTime.parse("08:00")) 
        || currentTime.equals(LocalTime.parse("10:00"))
        || currentTime.equals(LocalTime.parse("13:00")) 
        || currentTime.equals(LocalTime.parse("14:00"))
        || currentTime.equals(LocalTime.parse("19:00")) 
        || currentTime.equals(LocalTime.parse("21:00"))) {
      radius = 3.0;
    } else {
      radius = 5.0;
    }

    if (getRestaurantsRequest.getSearchFor().equals("")) {
      GetRestaurantsResponse restaurantsResponse = 
          new GetRestaurantsResponse(new ArrayList<Restaurant>());
      return restaurantsResponse;
    }

    CompletableFuture<List<Restaurant>> restaurants1;
    CompletableFuture<List<Restaurant>> restaurants2;
    CompletableFuture<List<Restaurant>> restaurants3;

    restaurants1 = restaurantRepositoryService.findRestaurantsByNameAsync(
        getRestaurantsRequest.getLatitude(),
        getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), 
        currentTime, radius);

    restaurants2 = restaurantRepositoryService.findRestaurantsByAttributesAsync(
        getRestaurantsRequest.getLatitude(),
        getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), 
        currentTime, radius);

    restaurants3 = restaurantRepositoryService.findRestaurantsByItemNameAsync(
        getRestaurantsRequest.getLatitude(),
        getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), 
        currentTime, radius);

    CompletableFuture<List<Restaurant>> restaurants4;

    restaurants4 = restaurantRepositoryService.findRestaurantsByItemAttributesAsync(
        getRestaurantsRequest.getLatitude(),
        getRestaurantsRequest.getLongitude(), getRestaurantsRequest.getSearchFor(), 
        currentTime, radius);

    CompletableFuture.allOf(restaurants1,restaurants2,restaurants3,restaurants4).join();

    List<Restaurant> restaurantList;

    restaurantList = restaurants1.get();

    restaurantList.addAll(restaurants2.get());
    restaurantList.addAll(restaurants3.get());
    restaurantList.addAll(restaurants4.get());
    
    List<Restaurant> restaurants = new ArrayList<>();
    Set<Restaurant> set = new LinkedHashSet<Restaurant>(restaurantList);

    restaurants = new ArrayList<>(set);
    GetRestaurantsResponse restaurantsResponse = new GetRestaurantsResponse(restaurants);

    return restaurantsResponse;

  }
}

