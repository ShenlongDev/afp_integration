from datetime import datetime, timedelta
import logging
from django.db.models import Sum, Avg, F, FloatField, Value, ExpressionWrapper, Case, When
from django.db.models.functions import TruncDate, ExtractWeekDay
from django.db.models.expressions import RawSQL
from django.utils.timezone import make_aware
from django.utils import timezone
import pytz

from integrations.models.models import POSSales, Weather
from integrations.models.xero.raw import XeroBudgetPeriodBalancesRaw
from core.models import Commentary, Site, Organisation
import calendar
logger = logging.getLogger(__name__)

def get_weeks_in_month(year, month):
    """Return a list of week start dates (YYYY-MM-DD) in the given month."""
    cal = calendar.monthcalendar(year, month)
    weeks = []
    for week in cal:
        # Find the first non-zero day (actual day in the month)
        for day in week:
            if day != 0:
                date = datetime(year, month, day).date()
                weeks.append(date.strftime('%Y-%m-%d'))
                break
    return weeks

def get_weekly_sales_and_weather(site_id=None):
    """Get weekly sales and weather data from local database models."""
    london_tz = pytz.UTC
    today = timezone.now().astimezone(london_tz).date()
    yesterday = today - timedelta(days=1)
    end_of_period = yesterday
    start_of_period = end_of_period - timedelta(days=6)
    
    # Create a date spine for the last 7 days
    date_spine = [(start_of_period + timedelta(days=i)) for i in range(7)]

    try:
        logger.info(f"Querying POSSales data for site_id: {site_id}")
        
        # Get commentary data for the period
        commentary_query = Commentary.objects.filter(
            site_id=site_id,
            created_at__gte = datetime.combine(timezone.now().astimezone(london_tz).date(), datetime.min.time(), tzinfo=london_tz) - timedelta(days=1),
        ).select_related('user').order_by('created_at')
        
        comments = [
            {"commentary": c.comments, "user": c.user.email}
            for c in commentary_query
        ]

        print(f"Comments: {comments}")

        # Combined query for the last 7 days' data
        sales_data = {}
        sales_query = POSSales.objects.filter(
            site_id=site_id,
            date_ntz__date__gte=start_of_period,
            date_ntz__date__lte=end_of_period
        ).annotate(
            sales_date=TruncDate('date_ntz')
        ).values('sales_date', 'currency').annotate(
            sales=Sum(
                Case(
                    When(
                        id__in=POSSales.objects.filter(
                            site_id=site_id,
                            date_ntz__date__gte=start_of_period,
                            date_ntz__date__lte=end_of_period
                        ).order_by('order_id', '-modified').distinct('order_id').values_list('id', flat=True),
                        then=F('net_amount')
                    ),
                    default=Value(0),
                    output_field=FloatField()
                )
            ),
            item_sales=Sum(
                Case(
                    When(
                        id__in=POSSales.objects.filter(
                            site_id=site_id,
                            date_ntz__date__gte=start_of_period,
                            date_ntz__date__lte=end_of_period
                        ).order_by('order_id', '-modified').distinct('order_id').values_list('id', flat=True),
                        then=F('item_net_amount')
                    ),
                    default=Value(0),
                    output_field=FloatField()
                )
            ),
            covers=Sum(
                Case(
                    When(
                        id__in=POSSales.objects.filter(
                            site_id=site_id,
                            date_ntz__date__gte=start_of_period,
                            date_ntz__date__lte=end_of_period
                        ).order_by('order_id', '-modified').distinct('order_id').values_list('id', flat=True),
                        then=F('covers')
                    ),
                    default=Value(0),
                    output_field=FloatField()
                )
            )
        ).order_by('sales_date')
        
        # Process query results to prepare this_week_data and last_week_data dictionaries
        this_week_data = {}
        for record in sales_query:
            date = record['sales_date']
            data = {
                'sales': float(record['sales'] or 0),
                'covers': float(record['covers'] or 0),
                'currency': record['currency'] or 'GBP'
            }
            this_week_data[date] = data

        # Query for last week's data
        last_week_start = start_of_period - timedelta(days=7)
        last_week_end = end_of_period - timedelta(days=7)

        last_week_query = POSSales.objects.filter(
            site_id=site_id,
            date_ntz__date__gte=last_week_start,
            date_ntz__date__lte=last_week_end
        ).annotate(
            sales_date=TruncDate('date_ntz')
        ).values('sales_date', 'currency').annotate(
            sales=Sum(
                Case(
                    When(
                        id__in=POSSales.objects.filter(
                            site_id=site_id,
                            date_ntz__date__gte=last_week_start,
                            date_ntz__date__lte=last_week_end
                        ).order_by('order_id', '-modified').distinct('order_id').values_list('id', flat=True),
                        then=F('net_amount')
                    ),
                    default=Value(0),
                    output_field=FloatField()
                )
            ),
            covers=Sum(
                Case(
                    When(
                        id__in=POSSales.objects.filter(
                            site_id=site_id,
                            date_ntz__date__gte=last_week_start,
                            date_ntz__date__lte=last_week_end
                        ).order_by('order_id', '-modified').distinct('order_id').values_list('id', flat=True),
                        then=F('covers')
                    ),
                    default=Value(0),
                    output_field=FloatField()
                )
            )
        ).order_by('sales_date')

        # Process last week's query results
        last_week_data = {}
        for record in last_week_query:
            date = record['sales_date']
            data = {
                'sales': float(record['sales'] or 0),
                'covers': float(record['covers'] or 0),
                'currency': record['currency'] or 'GBP'
            }
            last_week_data[date] = data

        # last year data
        last_year_start = start_of_period.replace(year=start_of_period.year - 1)
        last_year_end = end_of_period.replace(year=end_of_period.year - 1)

        last_year_query = POSSales.objects.filter(
            site_id=site_id,
            date_ntz__date__gte=last_year_start,
            date_ntz__date__lte=last_year_end
        ).annotate(
            sales_date=TruncDate('date_ntz')
        ).values('sales_date', 'currency').annotate(
            sales=Sum(
                Case(
                    When(
                        id__in=POSSales.objects.filter(
                            site_id=site_id,
                            date_ntz__date__gte=last_year_start,
                            date_ntz__date__lte=last_year_end
                        ).order_by('order_id', '-modified').distinct('order_id').values_list('id', flat=True),
                        then=F('net_amount')
                    ),
                    default=Value(0),
                    output_field=FloatField()
                )
            ),
            covers=Sum(
                Case(
                    When(
                        id__in=POSSales.objects.filter(
                            site_id=site_id,
                            date_ntz__date__gte=last_year_start,
                            date_ntz__date__lte=last_year_end
                        ).order_by('order_id', '-modified').distinct('order_id').values_list('id', flat=True),
                        then=F('covers')
                    ),
                    default=Value(0),
                    output_field=FloatField()
                )
            )
        ).order_by('sales_date')

        # Process last year's query results
        last_year_data = {}
        for record in last_year_query:
            date = record['sales_date']
            data = {
                'sales': float(record['sales'] or 0),
                'covers': float(record['covers'] or 0),
                'currency': record['currency'] or 'GBP'
            }
            last_year_data[date] = data

        # Query weather data with aggregations
        weather_by_date = {}
        weather_query = Weather.objects.filter(
            site_id=site_id,
            record_date__date__gte=start_of_period,
            record_date__date__lte=end_of_period
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
                    record_date__date__gte=start_of_period,
                    record_date__date__lte=end_of_period
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

        # Budget data
        period = end_of_period.strftime('%Y-%m')
        site = Site.objects.filter(id=site_id).first()
        orgnisation = Organisation.objects.filter(id=site.organisation_id).first()
        
        budget_query = XeroBudgetPeriodBalancesRaw.objects.filter(
            period=period,
            reporting_code_name='Revenue'
        )
        if not site.name == "Default Site":
            budget_query = budget_query.filter(tracking_category_option=site.name)
        else:
            budget_query = budget_query.filter(tenant_id=orgnisation.id)
        raw_data = budget_query.values('amount')

        total_budget = 0
        for record in raw_data:
            total_budget += float(record['amount'] or 0)
        

        year, month = map(int, period.split('-'))
        month_calendar = calendar.monthcalendar(year, month)
        sunday_count = sum(1 for week in month_calendar if week[calendar.SUNDAY] != 0)        
        
        weekly_budget = total_budget / sunday_count if sunday_count else 0
        print(f"Weekly budget: {weekly_budget}")

        daily_budgets = {
            0: weekly_budget * 0.107415,
            1: weekly_budget * 0.118575,
            2: weekly_budget * 0.127685,
            3: weekly_budget * 0.132044,
            4: weekly_budget * 0.171243,
            5: weekly_budget * 0.211193,
            6: weekly_budget * 0.131847
        }

    except Exception as e:
        logger.error(f"Error querying database: {str(e)}")
        logger.info("Falling back to sample data")
        
        # Define fallback daily budgets
        total_budget = 0  # Example fallback budget
        weekly_budget = total_budget / 4  # Assume 4 weeks in a month as a fallback
        daily_budgets = {
            0: weekly_budget * 0.107415,
            1: weekly_budget * 0.118575,
            2: weekly_budget * 0.127685,
            3: weekly_budget * 0.132044,
            4: weekly_budget * 0.171243,
            5: weekly_budget * 0.211193,
            6: weekly_budget * 0.131847
        }

    sales_to_budget = [
        {
            'date': date,
            'variance': (
                this_week_data.get(date, {'sales': 0}).get('sales', 0) -
                daily_budgets.get(date.weekday(), 0)
            )
        }
        for date in date_spine
    ]
    best_day = max(sales_to_budget, key=lambda x: x['variance'], default={'date': date_spine[0]})['date']
    
    # Build the final result using list comprehension instead of loop
    result = [
        {
            'DATE': date,
            'CURRENCY': this_week_data.get(date, {'currency': 'GBP'}).get('currency', 'GBP'),
            'SALES': this_week_data.get(date, {'sales': 0}).get('sales', 0),
            'LW_SALES': last_week_data.get(date - timedelta(days=7), {'sales': 0}).get('sales', 0),
            'LY_SALES': last_year_data.get(date - timedelta(days=7), {'sales': 0}).get('sales', 0),
            'SALES_CHANGE_PCT': (
                ((this_week_data.get(date, {'sales': 0}).get('sales', 0) - 
                 last_week_data.get(date - timedelta(days=7), {'sales': 0}).get('sales', 0)) / 
                 last_week_data.get(date - timedelta(days=7), {'sales': 0}).get('sales', 1)) * 100
                if last_week_data.get(date - timedelta(days=7), {'sales': 0}).get('sales', 0) > 0
                else 0
            ),
            'SALES_CHANGE_PCT_LY': (
                ((this_week_data.get(date, {'sales': 0}).get('sales', 0) - 
                 last_year_data.get(date - timedelta(days=7), {'sales': 0}).get('sales', 0)) / 
                 last_year_data.get(date - timedelta(days=7), {'sales': 0}).get('sales', 1)) * 100
                if last_year_data.get(date - timedelta(days=7), {'sales': 0}).get('sales', 0) > 0
                else 0
            ),
            'COVERS': this_week_data.get(date, {'covers': 1}).get('covers', 1),
            'BUDGET': daily_budgets.get(date.weekday(), 0),
            'BUDGET_VAR_PERCENTAGE': (
                ((this_week_data.get(date, {'sales': 0}).get('sales', 0) - 
                 daily_budgets.get(date.weekday(), 0)) / 
                 daily_budgets.get(date.weekday(), 1)) * 100
                if daily_budgets.get(date.weekday(), 0) > 0
                else 0
            ),
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
            'DESCRIPTION': weather_by_date.get(date, {'description': 'Partly cloudy'}).get('description'),
            "BEST_DAY": date == best_day
        }
        for date in date_spine
    ]
    
    logger.info(f"Generated report with {len(result)} days")
    return {
        'data': result,
        'comments': comments
    }