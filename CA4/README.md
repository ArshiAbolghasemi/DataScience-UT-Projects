# Data Science Assignment 4 - Deep Learning Projects

## Project Overview

This assignment consists of three comprehensive deep learning projects designed to provide hands-on experience with different neural network architectures and real-world applications:

1. **Multi-Layer Perceptron (MLP)** - Football match outcome prediction
2. **Convolutional Neural Network (CNN)** - Flower image classification  
3. **Recurrent Neural Network (RNN)** - Bitcoin price forecasting

## Project Structure

```
.
├── README.md
├── requirements.txt
├── cnn/
│   └── image_classification.ipynb
├── multi_layer_perceptron/
│   ├── fifa_world_cup.ipynb
│   └── matches.csv
└── rnn/
    ├── forecast_bitcoin_price.ipynb
    └── BTC-USD.csv
```

## Task 1: Multi-Layer Perceptron (Football Match Prediction)

### Project Overview
This task implements a Multi-Layer Perceptron (MLP) neural network to predict FIFA World Cup match outcomes using historical qualifier data. The project demonstrates fundamental deep learning concepts including data preprocessing, neural network architecture design, and sports analytics.

**File:** `multi_layer_perceptron/fifa_world_cup.ipynb`

## Task 2: Convolutional Neural Network (Flower Classification)

### Project Overview
This task explores computer vision and deep learning through flower species classification. Students build two different CNN approaches: a VGG-style network from scratch and a fine-tuned pre-trained ResNet model. The project demonstrates the power of transfer learning and compares different CNN architectures.

**Key Learning Objectives:**
- Understanding CNN architecture (convolution, pooling, fully connected layers)
- Image preprocessing and data augmentation techniques
- Transfer learning vs. training from scratch
- Model comparison and evaluation in computer vision
- Handling multi-class image classification problems

**Real-World Application:**
Flower classification has applications in botanical research, gardening apps, and environmental monitoring. The techniques learned here apply broadly to any image classification task, from medical imaging to autonomous vehicles.

**File:** `cnn/image_classification.ipynb`

## Task 3: Recurrent Neural Network (Bitcoin Price Prediction)

### Project Overview
This task focuses on time series forecasting using Recurrent Neural Networks (RNNs) to predict Bitcoin prices. Students work with financial data to understand temporal dependencies and sequential modeling. The project includes feature engineering from OHLCV data and explores the impact of different sequence lengths on model performance

**File:** `rnn/forecast_bitcoin_price.ipynb`
