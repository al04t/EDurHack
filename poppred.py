import pandas as pd
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from statsmodels.tsa.statespace.sarimax import SARIMAX

def load_data(file_path):
    dfs = []
    for fp in file_path:
        df = pd.read_csv(fp)
        dfs.append(df)
    combined_df = pd.concat(dfs, ignore_index=True)
    return combined_df

file_path = [
    'Dataset\cleanData\adjusted_sightings_by_grid_per_year_2018.csv',
    'Dataset\cleanData\adjusted_sightings_by_grid_per_year_2019.csv',
    'Dataset\cleanData\adjusted_sightings_by_grid_per_year_2020.csv',
    'Dataset\cleanData\adjusted_sightings_by_grid_per_year_2021.csv',
    'Dataset\cleanData\adjusted_sightings_by_grid_per_year_2022.csv',
    'Dataset\cleanData\adjusted_sightings_by_grid_per_year_2023.csv',
    'Dataset\cleanData\adjusted_sightings_by_grid_per_year_2024.csv',
    'Dataset\cleanData\adjusted_sightings_by_grid_per_year_2025.csv'
]

pop = load_data(file_path)
hphd = pd.read_csv('Dataset\cleanData\population_data_woodchucks.csv')

pop['date'] = pd.to_datetime(pop[['year', 'month']].assign(day=1))





ts_data = monthly_data.set_index('date')

train_size = len(ts_data) - 12
train = ts_data[:train_size]
test = ts_data[train_size:]

print("\n" + "-"*70)
print("MODEL 1: Population Index with HPHD as exogenous variable")
print("-"*70)

model = SARIMAX(
    train['population_index_scaled'],
    exog=train[['hphd']],
    order=(1, 1, 1),                    
    seasonal_order=(1, 1, 1, 12),       
    enforce_stationarity=False,
    enforce_invertibility=False
)

print("Fitting Model 1...")
model1_fit = model.fit(disp=False, maxiter=200)
print(model1_fit.summary())

# Predictions
pred_test = model1_fit.forecast(steps=len(test), exog=test[['hphd']])

# Evaluate
mae1 = mean_absolute_error(test['population_index_scaled'], pred_test)
rmse1 = np.sqrt(mean_squared_error(test['population_index_scaled'], pred_test))
r2_1 = r2_score(test['population_index_scaled'], pred_test)

print(f"\nModel 1 Test Performance:")
print(f"  MAE:  {mae1:.2f}")
print(f"  RMSE: {rmse1:.2f}")
print(f"  R²:   {r2_1:.3f}")

forecast_months = 24
last_date = ts_data.index[-1]
future_dates = pd.date_range(start=last_date + pd.DateOffset(months=1), 
                             periods=forecast_months, freq='MS')

recent_hphd = train['hphd'].tail(24)  # Last 2 years
mean_hphd = recent_hphd.mean()
trend_hphd = (recent_hphd.iloc[-1] - recent_hphd.iloc[0]) / len(recent_hphd)

future_hphd_constant = pd.DataFrame({
    'hphd': [mean_hphd] * forecast_months
}, index=future_dates)

pred_constant = model1_fit.forecast(steps=forecast_months, exog=future_hphd_constant)

fig, axes = plt.subplots(2, 1, figsize=(18, 12))

ax1 = axes[0]
ax1.plot(ts_data.index, ts_data['population_index_scaled'], 
         label='Historical Population Index', linewidth=2.5, marker='o', 
         color='black', markersize=3)
ax1.plot(test.index, pred_test, 
         label='Test Predictions', linewidth=2, linestyle='--', 
         marker='s', color='blue', markersize=4)
ax1.plot(future_dates, pred_constant, 
         label='Forecast: Constant HPHD', linewidth=2.5, linestyle=':', 
         color='green')
ax1.axvline(x=test.index[0], color='red', linestyle='-', alpha=0.3, 
            linewidth=2, label='Test Period Start')
ax1.set_title('Groundhog Population Index: Historical and Forecast', 
              fontsize=16, fontweight='bold')
ax1.set_xlabel('Date', fontsize=12)
ax1.set_ylabel('Population Index (0-100)', fontsize=12)
ax1.legend(loc='best', fontsize=10)
ax1.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('population_forecasts.png', dpi=300, bbox_inches='tight')
plt.show()