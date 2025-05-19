# Introduction

This repository contains the implementation of three machine learning tasks:
1. Cancer Survival Prediction (Classification)
2. Bike Rental Prediction (Regression)
3. Movie Recommendation System

## Task 1: Cancer Survival Prediction (Classification)

### Objective
Develop a binary classification model to predict cancer patient survival status (1 = Alive, 0 = Deceased) using patient demographics, diagnosis details, treatment history, and examination results.

### Approach
1. **Data Exploration & Understanding**: Analysis of features like Birth_Date, Weight, Cancer_Type, and their relationship to survival status
2. **Preprocessing & Feature Engineering**: Handling missing values, encoding categorical features, scaling, date processing
3. **Model Development & Selection**: Experimentation with various classification algorithms
4. **Evaluation**: Primary metric is Accuracy, with additional tracking of Precision, Recall, and F1-Score

## Task 2: Bike Rental Prediction (Regression)

### Objective
Develop a regression model to predict the total number of bike rentals (`total_users`) based on weather conditions, seasonal information, and calendar details.

### Approach
1. **Data Exploration**: Analysis of how features like season, temperature, humidity affect rental activity
2. **Feature Selection**: Using statistical methods to select relevant features
3. **Preprocessing**: Encoding categorical variables, scaling numerical features
4. **Model Development**: Experimenting with regression algorithms
5. **Evaluation**: Using MSE, RMSE, R-squared, MAPE, and MAE as metrics

## Task 3: Movie Recommendation System

### Objective
Design a recommendation system to predict user ratings for movies they have not yet rated.

### Approach
1. **Data Exploration**: Analysis of user-movie ratings and trust relationships
2. **Preprocessing**: Handling missing data, normalizing ratings
3. **Model Development**: Building collaborative filtering or matrix factorization models
4. **Evaluation**: Using RMSE, MAE, MSE, R2 as metrics

## Evaluation Guidelines

- **Submission Format**: Each task requires predicting values and submitting in a specified format
- **Performance Metrics**:
- Classification: Accuracy (primary), Precision, Recall, F1-Score
- Regression: MSE (primary), RMSE, R-squared, MAPE, MAE
- Recommendation: RMSE, MAE, MSE, R2
- **Restrictions**: No deep learning or pre-trained models allowed

## Notes

1. All models use traditional machine learning algorithms only (no deep learning)
