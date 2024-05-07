# Trading Matching Engine With Monitoring System


## Getting Started
#### Java version 11.0.8
#### Spring Boot version 2.7.11
#### ReactJS version 18.x

## Cloning repositories
#### Server
```bash
git clone https://github.com/ITITIU19087/new-order-matching.git
```

#### Monitoring Interface (MI)
```bash
git clone https://github.com/ITITIU19087/monitoring-interface-.git
```

Remember to install all the necessary applications before running the program. (Encourage opening 2 terminal browsers for MI and server)

## Server
### Set up .properties file
```bash
server.port=

spring.datasource.url=
spring.datasource.username=
spring.datasource.password=
spring.jpa.hibernate.ddl-auto=
spring.jpa.show-sql=
spring.jpa.properties.hibernate.format_sql=
```

Please make sure that there are no programs running on your server.port value.

### Maven commands
Using this command for install all dependencies in pom.xml file
```bash
mvn clean build
```

Using this command to start the system
```bash
mvn spring-boot:run
```

Please see test data in ``test_orders.txt`` file to test the APIs.

## Monitoring interface
Install packages
```bash
npm install
# or
yarn
```

Start
```bash
npm start
# or
yarn start
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

## Learn More

### Spring Boot
To learn more about Spring Boot, take a look at the following resources:

- [Spring Boot Guide](https://spring.io/guides) - learn about simple Spring Boot projects.
- [Learn Spring Boot](https://www.baeldung.com/spring-boot) - an interactive Boot tutorial.

You can check out [Spring Boot GitHub repository](https://github.com/spring-projects/spring-boot) - your feedback and contributions are welcome!

### React
To learn more about React, take a look at the following resources:

- [React Documentation](https://react.dev/) - learn about React features.
- [Learn React](https://react.dev/learn) - an interactive React tutorial.

You can check out [React GitHub repository](https://github.com/facebook/react/releases) - your feedback and contributions are welcome!