# DataScience-UT-Projects
This repository contains all projects for Data Science course in UT - Spring 2025.

## Projects Overview

### [CA0: Statistical Analysis Projects](CA0/)
1. **Roulette Simulation and Profit Analysis**
   - Simulates roulette gameplay and analyzes profit outcomes using statistical methods.
2. **2016 USA Presidential Election Prediction**
   - Predictive modeling of election results based on demographic and polling data.
3. **Drug Safety Test Analysis**
   - Statistical evaluation of clinical trial data for drug safety assessment.

### [CA1: Data Visualization and Sampling](CA1/)
1. **Langevin Dynamics Sampling Implementation**
   - Implementation of stochastic sampling method for molecular systems.
2. **Interactive Airbnb Data Dashboards**
   - Tableau dashboards exploring pricing, availability, and location trends in Airbnb listings.

### [CA2: Darooghe - Payment Processing Pipeline](CA2/)
- Domain-driven payment processing system with:
  - Real-time fraud detection
  - Batch analytics
  - Commission analysis
  - Transaction pattern insights
- Architecture includes:
  - Domain layer (core business logic)
  - Application layer (use cases)
  - Infrastructure layer (implementations)

### [CA3: Machine Learning Projects](CA3/)
1. **Cancer Survival Prediction (Classification)**
   - Binary classification predicting patient survival (Alive/Deceased) using:
     - Patient demographics
     - Diagnosis details
     - Treatment history
     - Examination results
   - Traditional ML algorithms only
2. **Bike Rental Prediction (Regression)**
   - Regression model to predict bike rental demand based on:
     - Temporal features
     - Weather conditions
     - Seasonal patterns
3. **Movie Recommendation System**
   - Collaborative filtering system for personalized movie recommendations

### [CA4: Deep Learning Projects](CA4/)

1. **Multi-Layer Perceptron (MLP) - Football Match Prediction**
   - Predicts FIFA World Cup match outcomes using Qatar 2022 qualifier data
   - Features team statistics and performance indicators
   - Includes complete tournament simulation
   - Demonstrates fundamental neural network concepts and sports analytics

2. **Convolutional Neural Network (CNN) - Flower Classification**
   - Multi-class flower species classification using computer vision
   - Compares two approaches:
     - VGG-style CNN built from scratch
     - Transfer learning with pre-trained ResNet
   - Includes data augmentation and comprehensive model evaluation
   - Explores the effectiveness of transfer learning vs. training from scratch

3. **Recurrent Neural Network (RNN) - Bitcoin Price Forecasting**
   - Time series prediction of cryptocurrency prices using historical OHLCV data
   - Custom feature engineering for profit/loss indicators
   - Experiments with different sequence lengths (30, 60, 90 days)
   - LSTM implementation for capturing long-term dependencies

### [CA5: Advanced Data Science Tasks](CA5/)

1. **Video Game Reviews – Semi-Supervised Score Prediction**

   * Predicts review scores from limited labeled and large unlabeled text data
   * Explores semi-supervised and active learning methods
   * Addresses challenges of scarce labeled data in user-generated content

2. **Semantic Search – Persian Q\&A Retrieval**

   * Builds a semantic search system using embeddings and vector databases
   * Focuses on Persian-language questions and answers from NiniSite forum
   * Implements ranking and reranking to improve answer relevance

3. **LLM for Multiple Choice Questions – SWAG Dataset**

   * Uses large language models for complex multiple-choice reasoning
   * Explores in-context learning, fine-tuning, and prompt engineering
   * Tests models on contextual and commonsense inference tasks

4. **Football Image Segmentation – Unsupervised Clustering**

   * Applies unsupervised pixel clustering for segmenting football players
   * Addresses the challenge of expensive pixel-level annotations
   * Evaluates segmentation quality with IoU and Dice coefficient metrics
   * Uses the Football Player Segmentation dataset from Kaggle
