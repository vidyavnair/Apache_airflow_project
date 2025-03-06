                                                          
                                                  
                                                  
# Apache Airflow Docker Project

## Overview:

 This project sets up Apache Airflow using Docker and includes DAGs for login, category, and product task using Object-Oriented Programming concepts.

## Features:

Containerized Apache Airflow setup using Docker

DAGs for

* User Login

* Category Data Management

* Product Data Management

Implemented using OOP principles for better maintainability

## Prerequisites:

Your system needs to have:

* Docker

* Docker Compose

## Installation & Setup:

   1.Clone the repository:

        git clone <repository-url>
        cd <project-folder>

        
   2.Start Airflow using Docker Compose:

        docker-compose up -d

   3.Access the Airflow web UI at:

        http://localhost:8086
    
        Default credentials:

        Username: airflow
        Password: airflow

   4.To stop the containers:
   
        docker-compose down

## DAGs Explanation:

  * Logined with username and passwords
  * Categories class inherits the Login class, login in to api, print token and print all the categories available in the api
  * Products class inherit the Category class, login into the api, collect all the categories, fetch all products for each category, saved all the  products into the database

