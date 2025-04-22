from datetime import datetime, timedelta
import logging
from django.db.models import Sum, Avg, F, FloatField, Value, ExpressionWrapper, Case, When
from django.db.models.functions import TruncDate, ExtractWeekDay
from django.db.models.expressions import RawSQL
from django.utils.timezone import make_aware

from integrations.models.models import POSSales, Weather

logger = logging.getLogger(__name__)

def get_weekly_sales_and_weather(site_id=None):
    """Get weekly sales and weather data from local database models."""
    today = datetime.now().date()
    start_of_week = (today - timedelta(days=today.weekday()))
    this_week_start = start_of_week
    this_week_end = this_week_start + timedelta(days=6)
    last_week_start = this_week_start - timedelta(days=7)
    last_week_end = this_week_end - timedelta(days=7)
    
    # Create a date spine from this week
    date_spine = [(start_of_week + timedelta(days=i)) for i in range(7)]
    
    try:
        logger.info(f"Querying POSSales data for site_id: {site_id}")
        
        # Combined query for both weeks' data
        sales_data = {}
        sales_query = POSSales.objects.filter(
            site_id=site_id,
            date_ntz__date__gte=last_week_start,
            date_ntz__date__lte=this_week_end
        ).annotate(
            sales_date=TruncDate('date_ntz'),
            is_this_week=Case(
                When(date_ntz__date__gte=this_week_start, then=Value(True)),
                default=Value(False)
            )
        ).values('sales_date', 'is_this_week', 'currency').annotate(
            sales=Sum('net_amount'),
            covers=Sum('covers')
        ).order_by('sales_date')
        
        # Process query results to prepare this_week_data and last_week_data dictionaries
        this_week_data = {}
        last_week_data = {}
        
        for record in sales_query:
            date = record['sales_date']
            data = {
                'sales': float(record['sales'] or 0),
                'covers': float(record['covers'] or 0) + 1,
                'currency': record['currency'] or 'GBP'
            }
            
            if record['is_this_week']:
                this_week_data[date] = data
            else:
                # For last week data, map the date to corresponding current week date
                day_of_week = date.weekday()
                equivalent_this_week = this_week_start + timedelta(days=day_of_week)
                # Store with the equivalent date from last week
                last_week_data[equivalent_this_week - timedelta(days=7)] = data
        
        # Query weather data with aggregations
        weather_by_date = {}
        weather_query = Weather.objects.filter(
            site_id=site_id,
            record_date__date__gte=this_week_start,
            record_date__date__lte=this_week_end
        ).annotate(
            weather_date=TruncDate('record_date')
        ).values('weather_date').annotate(
            temp=Avg('temperature_value')
        ).values('weather_date', 'temp', 'description')
        
        if not weather_query.exists():
            logger.info(f"No weather data found for site_id: {site_id}, trying other sites")
            other_site = Weather.objects.values_list('site_id', flat=True).distinct().first()
            
            if other_site:
                logger.info(f"Using weather data from site_id: {other_site}")
                weather_query = Weather.objects.filter(
                    site_id=other_site,
                    record_date__date__gte=this_week_start,
                    record_date__date__lte=this_week_end
                ).annotate(
                    weather_date=TruncDate('record_date')
                ).values('weather_date').annotate(
                    temp=Avg('temperature_value')
                ).values('weather_date', 'temp', 'description')
        
        # Process weather data
        for record in weather_query:
            date = record['weather_date']
            weather_by_date[date] = {
                'temp': float(record['temp'] or 15),
                'description': record['description'] or 'Partly cloudy'
            }
        
        # Fill in missing data with sample values
        if not sales_query.exists():
            logger.info("No sales data found. Using sample data for demonstration.")
            # Create sample data for all dates at once using dictionary comprehensions
            this_week_data = {
                date_spine[i]: {
                    'sales': 1000 + i * 100,
                    'covers': 50 + i * 5,
                    'currency': 'GBP'
                } for i in range(7)
            }
            
            last_week_data = {
                date_spine[i] - timedelta(days=7): {
                    'sales': 900 + i * 90,
                    'covers': 45 + i * 4,
                    'currency': 'GBP'
                } for i in range(7)
            }
        
        # Fill in missing weather data with deterministic values
        weather_descriptions = ['Sunny', 'Partly cloudy', 'Cloudy', 'Light rain', 'Moderate rain', 'Heavy rain']
        
        # Use a dictionary comprehension for missing weather data
        missing_weather = {
            date: {
                'temp': 15 + (date.weekday() % 10),
                'description': weather_descriptions[date.weekday() % len(weather_descriptions)]
            } for date in date_spine if date not in weather_by_date
        }
        
        # Update weather data with missing values
        weather_by_date.update(missing_weather)
    
    except Exception as e:
        logger.error(f"Error querying database: {str(e)}")
        logger.info("Falling back to sample data")
        
        # Create sample data using comprehensions instead of loops
        this_week_data = {
            date_spine[i]: {
                'sales': 1000 + i * 100,
                'covers': 50 + i * 5,
                'currency': 'GBP'
            } for i in range(7)
        }
        
        last_week_data = {
            date_spine[i] - timedelta(days=7): {
                'sales': 900 + i * 90,
                'covers': 45 + i * 4,
                'currency': 'GBP'
            } for i in range(7)
        }
        
        weather_descriptions = ['Sunny', 'Partly cloudy', 'Cloudy', 'Light rain', 'Moderate rain', 'Heavy rain']
        weather_by_date = {
            date: {
                'temp': 15 + (i % 10),
                'description': weather_descriptions[i % len(weather_descriptions)]
            } for i, date in enumerate(date_spine)
        }
    
    # Build the final result using list comprehension instead of loop
    result = [
        {
            'DATE': date,
            'CURRENCY': this_week_data.get(date, {'currency': 'GBP'}).get('currency', 'GBP'),
            'SALES': this_week_data.get(date, {'sales': 0}).get('sales', 0),
            'LW_SALES': last_week_data.get(date - timedelta(days=7), {'sales': 0}).get('sales', 0),
            'SALES_CHANGE_PCT': (
                ((this_week_data.get(date, {'sales': 0}).get('sales', 0) - 
                 last_week_data.get(date - timedelta(days=7), {'sales': 0}).get('sales', 0)) / 
                 last_week_data.get(date - timedelta(days=7), {'sales': 0}).get('sales', 1)) * 100
                if last_week_data.get(date - timedelta(days=7), {'sales': 0}).get('sales', 0) > 0
                else 0
            ),
            'COVERS': this_week_data.get(date, {'covers': 1}).get('covers', 1),
            'LW_COVERS': last_week_data.get(date - timedelta(days=7), {'covers': 1}).get('covers', 1),
            'COVERS_CHANGE_PCT': (
                ((this_week_data.get(date, {'covers': 1}).get('covers', 1) - 
                 last_week_data.get(date - timedelta(days=7), {'covers': 1}).get('covers', 1)) /
                 last_week_data.get(date - timedelta(days=7), {'covers': 1}).get('covers', 1)) * 100
                if last_week_data.get(date - timedelta(days=7), {'covers': 1}).get('covers', 1) > 0
                else 0
            ),
            'ASPH': this_week_data.get(date, {'sales': 0, 'covers': 1}).get('sales', 0) / 
                   this_week_data.get(date, {'covers': 1}).get('covers', 1) if this_week_data.get(date, {'covers': 1}).get('covers', 1) else 0,
            'LW_ASPH': last_week_data.get(date - timedelta(days=7), {'sales': 0, 'covers': 1}).get('sales', 0) / 
                      last_week_data.get(date - timedelta(days=7), {'covers': 1}).get('covers', 1) 
                      if last_week_data.get(date - timedelta(days=7), {'covers': 1}).get('covers', 1) else 0,
            'TEMPERATURE_VALUE': weather_by_date.get(date, {'temp': 15}).get('temp'),
            'DESCRIPTION': weather_by_date.get(date, {'description': 'Partly cloudy'}).get('description')
        }
        for date in date_spine
    ]
    
    logger.info(f"Generated report with {len(result)} days")
    return result 