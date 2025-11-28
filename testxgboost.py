import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV, KFold
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import xgboost as xgb

# --- 1. LOAD AND PREP DATA ---

# Load Dataset
df = pd.read_csv('houses_dataset.csv') 

target_col = 'financial_lastSoldPrice'
description_col = 'basic_info_description'

y = df[target_col].copy()
columns_to_exclude = [target_col, description_col]
X = df.drop(columns=columns_to_exclude)

# Log transform the target variable
y_log = np.log1p(y)

# Create a combined DataFrame for easy binning and indexing
df_full = X.copy()
df_full['y_log'] = y_log
df_full['y'] = y # Keep the original price for binning

print(f"Dataset shape: {X.shape}")
print("-" * 50)

# --- 2. MANUAL STRATIFICATION BINNING ---

# Create 5 bins based on the UN-TRANSFORMED target variable (y) using quantiles.
num_bins = 5 
df_full['price_bin'] = pd.qcut(df_full['y'], q=num_bins, labels=False, duplicates='drop')
y_stratify = df_full['price_bin']

print(f"Created {num_bins} price bins for stratification.")
print("Price Bin Distribution:")
print(y_stratify.value_counts().sort_index())
print("-" * 50)

# --- 3. MANUAL STRATIFIED 64/16/20 SPLITTING ---

# Initialize lists to store indices for each split
train_indices = []
val_indices = []
test_indices = []

# Iterate through each price bin and split independently
bins = df_full['price_bin'].unique()

for bin_label in bins:
    bin_data = df_full[df_full['price_bin'] == bin_label]
    
    # 1. Split the bin data into Test (20%) and Train/Validate (80%)
    idx_train_val, idx_test = train_test_split(
        bin_data.index, 
        test_size=0.20, 
        random_state=42, 
        shuffle=True
    )
    
    # 2. Split the 80% chunk into Train (64%) and Validate (16%)
    idx_train, idx_val = train_test_split(
        idx_train_val, 
        test_size=0.20, # 20% of the 80% chunk = 16% total
        random_state=42, 
        shuffle=True
    )
    
    # Collect the indices
    train_indices.extend(idx_train)
    val_indices.extend(idx_val)
    test_indices.extend(idx_test)


# --- 4. FINAL DATA ASSEMBLY ---

# Assemble the final stratified feature and target sets (NumPy arrays)
X_train = X.loc[train_indices].values
y_train_log = y_log.loc[train_indices].values

X_val = X.loc[val_indices].values
y_val_log = y_log.loc[val_indices].values

X_test = X.loc[test_indices].values
y_test_log = y_log.loc[test_indices].values

# Combined Train+Val for Hyperparameter Search (80% of data)
X_train_full = np.vstack([X_train, X_val])
y_train_full_log = np.concatenate([y_train_log, y_val_log])
# Bins for the full 80% set (used for StratifiedKFold)
y_stratify_train_val_arr = np.concatenate([y_stratify.loc[train_indices].values, y_stratify.loc[val_indices].values])


print(f"Train size: {X_train.shape[0]} (64% stratified)")
print(f"Validate size: {X_val.shape[0]} (16% stratified)")
print(f"Test size: {X_test.shape[0]} (20% stratified)")
print("-" * 50)

# --- 5. HYPERPARAMETER SEARCH WITH STRATIFIED K-FOLD ---

# NOTE: Use your full, optimized grid here. This is a smaller example grid.
param_grid = {
    'max_depth': [3, 4], # Keep it shallow
    'learning_rate': [0.01, 0.05], # Lower learning rate
    'min_child_weight': [10, 20], # Higher values prevent overfitting to specific houses
    'subsample': [0.6, 0.8],
    'colsample_bytree': [0.6, 0.8],
    'reg_alpha': [0.1, 1.0], # L1 Regularization (Lasso)
    'reg_lambda': [1.0, 10.0], # L2 Regularization (Ridge)
}

print("Max Price Train:", np.expm1(y_train_log).max())
print("Max Price Val:  ", np.expm1(y_val_log).max()) 

base_model = xgb.XGBRegressor(
    objective='reg:squarederror',
    eval_metric='rmse',
    n_estimators=500,
    random_state=42,
    n_jobs=1,
)

# Define the K-Fold CV object (regular, not stratified, since this is regression)
cv = KFold(n_splits=5, shuffle=True, random_state=42)

print("Starting hyperparameter search with 5-fold CV...")

grid_search = GridSearchCV(
    estimator=base_model,
    param_grid=param_grid,
    cv=cv, 
    scoring='neg_root_mean_squared_error', # Optimize for RMSLE
    n_jobs=-1,
    verbose=1,
)

# Fit the grid search (no groups parameter needed for regular KFold)
grid_search.fit(
    X_train_full, 
    y_train_full_log
)

print("-" * 50)
print("HYPERPARAMETER SEARCH RESULTS")
print(f"Best parameters: {grid_search.best_params_}")
print(f"Best CV RMSLE: {-grid_search.best_score_:.6f}")
print("-" * 50)

# --- 6. FINAL MODEL TRAINING AND EVALUATION ---

# A. Final Model Training using best parameters and Early Stopping
best_model = xgb.XGBRegressor(
    **grid_search.best_params_, # Ensure you run grid search with the new regularization params
    objective='reg:squarederror',
    n_estimators=10000, # High number
    early_stopping_rounds=100, # Give it patience
    random_state=42,
    n_jobs=-1
)


# Use the 64% Train set for training and the 16% Val set for early stopping
eval_set_final = [(X_train, y_train_log), (X_val, y_val_log)] 

best_model.fit(
    X_train, y_train_log, 
    eval_set=eval_set_final,
    verbose=False
)

# B. Evaluation Function
def calculate_metrics(y_true_log, y_pred_log, set_name, model):
    # Inverse transform to original dollars
    y_true = np.expm1(y_true_log)
    y_pred = np.expm1(y_pred_log)

    # Metrics on original scale
    mae = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2 = r2_score(y_true, y_pred)
    
    # RMSLE (Root Mean Squared Log Error) on log scale
    rmsle = np.sqrt(mean_squared_error(y_true_log, y_pred_log))
    
    print(f"\n{set_name} Performance (N Estimators: {model.best_iteration}):")
    print(f"  MAE:    ${mae:,.2f}")
    print(f"  RMSE:   ${rmse:,.2f}")
    print(f"  RMSLE:  {rmsle:.6f}")
    print(f"  R²:     {r2:.4f}")

# C. Calculate Metrics
y_pred_train_log = best_model.predict(X_train, iteration_range=(0, best_model.best_iteration))
calculate_metrics(y_train_log, y_pred_train_log, "Training Set (Final)", best_model)

y_pred_val_log = best_model.predict(X_val, iteration_range=(0, best_model.best_iteration))
calculate_metrics(y_val_log, y_pred_val_log, "Validation Set (Early Stopped)", best_model)

y_pred_test_log = best_model.predict(X_test, iteration_range=(0, best_model.best_iteration))
calculate_metrics(y_test_log, y_pred_test_log, "Test Set (Final Generalization)", best_model)