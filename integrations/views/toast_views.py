from decimal import Decimal
from django.db.models import Sum, Case, When, Value, F, Q, DecimalField
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from integrations.models.toast.raw import (
    ToastOrder, ToastCheck, ToastSelection, ToastGeneralLocation, 
    ToastDaySchedule, ToastWeeklySchedule, ToastJoinedOpeningHours, 
    ToastRevenueCenter, ToastRestaurantService, ToastSalesCategory, 
    ToastDiningOption, ToastServiceArea, ToastPayment
)
from rest_framework import viewsets
from integrations.serializers.toast import (
    ToastOrderSerializer, ToastCheckSerializer, ToastSelectionSerializer,
    ToastGeneralLocationSerializer, ToastDayScheduleSerializer, ToastWeeklyScheduleSerializer,
    ToastJoinedOpeningHoursSerializer, ToastRevenueCenterSerializer, ToastRestaurantServiceSerializer,
    ToastSalesCategorySerializer, ToastDiningOptionSerializer, ToastServiceAreaSerializer,
    ToastPaymentSerializer
)


class SalesSummaryAPIView(APIView):
    """
    GET API endpoint that accepts startdate, enddate, option, and optionally restaurant_guid.
    Example URL: /sales-summary/?startdate=20250301&enddate=20250303&option=Netsales
    Example with restaurant filter: /sales-summary/?startdate=20250301&enddate=20250303&option=Netsales&restaurant_guid=9cfaffa2-0592-4f72-8b3e-423c879b9150
    """
    def get(self, request, format=None):
        # Get parameters from URL query parameters.
        start_date = request.query_params.get('startdate')
        end_date = request.query_params.get('enddate')
        option = request.query_params.get('option')
        restaurant_guid = request.query_params.get('restaurant_guid')

        # Validate presence of parameters.
        if not start_date or not end_date or not option:
            return Response(
                {"error": "startdate, enddate, and option parameters are required."},
                status=status.HTTP_400_BAD_REQUEST
            )

        option = option.lower()
        if option not in ["netsales", "totalamounts"]:
            return Response(
                {"error": "option must be either 'Netsales' or 'TotalAmounts'."},
                status=status.HTTP_400_BAD_REQUEST
            )

        try:
            start_date_int = int(start_date)
            end_date_int = int(end_date)
        except ValueError:
            return Response(
                {"error": "startdate and enddate must be numeric in the format YYYYMMDD."},
                status=status.HTTP_400_BAD_REQUEST
            )
            
        # Validate that the start date is not after the end date.
        if start_date_int > end_date_int:
            return Response(
                {"error": "startdate should be less than or equal to enddate."},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # (Optional) Validate that the dates are exactly in YYYYMMDD format (8 digits)
        if len(start_date) != 8 or len(end_date) != 8:
            return Response(
                {"error": "startdate and enddate must be in the format YYYYMMDD."},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Determine which field to aggregate.
        field_name = "order_net_sales" if option == "netsales" else "toast_sales"

        # Build a base queryset with date range filters
        date_filter = Q(business_date__gte=start_date_int, business_date__lte=end_date_int) | \
                      Q(refund_business_date__gte=str(start_date_int), refund_business_date__lte=str(end_date_int))
        
        # Add restaurant_guid filter if provided
        if restaurant_guid:
            orders = ToastOrder.objects.filter(date_filter, restaurant_guid=restaurant_guid)
        else:
            orders = ToastOrder.objects.filter(date_filter)

        # Aggregate primary amount and refund adjustment.
        result = orders.aggregate(
            total_primary=Sum(
                Case(
                    When(business_date__gte=start_date_int, business_date__lte=end_date_int, then=F(field_name)),
                    default=Value(Decimal("0.00")),
                    output_field=DecimalField()
                )
            ),
            total_refund=Sum(
                Case(
                    # Only subtract refunds if the refund_business_date is in range and
                    # the order's original business_date is different (i.e. the refund belongs to another day)
                    When(
                        Q(refund_business_date__gte=str(start_date_int)) &
                        Q(refund_business_date__lte=str(end_date_int)) &
                        ~Q(business_date=F('refund_business_date')),
                        then=F('total_refunds')
                    ),
                    default=Value(Decimal("0.00")),
                    output_field=DecimalField()
                )
            )
        )

        total_primary = result.get("total_primary") or Decimal("0.00")
        total_refund = result.get("total_refund") or Decimal("0.00")
        total_value = total_primary - total_refund

        # Optional: ensure that the total does not go negative.
        if total_value < 0:
            total_value = Decimal("0.00")

        data = {
            "startdate": start_date,
            "enddate": end_date,
            "option": option,
            "total": str(total_value)
        }
        
        # Include restaurant_guid in the response if it was provided
        if restaurant_guid:
            data["restaurant_guid"] = restaurant_guid
            
        return Response(data, status=status.HTTP_200_OK)

class ToastOrderViewSet(viewsets.ModelViewSet):
    queryset = ToastOrder.objects.all()
    serializer_class = ToastOrderSerializer
    filterset_fields = ['tenant_id', 'order_guid', 'business_date']

class ToastCheckViewSet(viewsets.ModelViewSet):
    queryset = ToastCheck.objects.all()
    serializer_class = ToastCheckSerializer
    filterset_fields = ['tenant_id', 'check_guid', 'order']

class ToastSelectionViewSet(viewsets.ModelViewSet):
    queryset = ToastSelection.objects.all()
    serializer_class = ToastSelectionSerializer
    filterset_fields = ['tenant_id', 'selection_guid', 'toast_check']

class ToastGeneralLocationViewSet(viewsets.ModelViewSet):
    queryset = ToastGeneralLocation.objects.all()
    serializer_class = ToastGeneralLocationSerializer
    filterset_fields = ['tenant_id', 'guid', 'archived']

class ToastDayScheduleViewSet(viewsets.ModelViewSet):
    queryset = ToastDaySchedule.objects.all()
    serializer_class = ToastDayScheduleSerializer
    filterset_fields = ['tenant_id', 'guid', 'restaurant']

class ToastWeeklyScheduleViewSet(viewsets.ModelViewSet):
    queryset = ToastWeeklySchedule.objects.all()
    serializer_class = ToastWeeklyScheduleSerializer
    filterset_fields = ['tenant_id', 'restaurant']

class ToastJoinedOpeningHoursViewSet(viewsets.ModelViewSet):
    queryset = ToastJoinedOpeningHours.objects.all()
    serializer_class = ToastJoinedOpeningHoursSerializer
    filterset_fields = ['tenant_id', 'restaurant']

class ToastRevenueCenterViewSet(viewsets.ModelViewSet):
    queryset = ToastRevenueCenter.objects.all()
    serializer_class = ToastRevenueCenterSerializer
    filterset_fields = ['tenant_id', 'revenue_center_guid']

class ToastRestaurantServiceViewSet(viewsets.ModelViewSet):
    queryset = ToastRestaurantService.objects.all()
    serializer_class = ToastRestaurantServiceSerializer
    filterset_fields = ['tenant_id', 'service_guid']

class ToastSalesCategoryViewSet(viewsets.ModelViewSet):
    queryset = ToastSalesCategory.objects.all()
    serializer_class = ToastSalesCategorySerializer
    filterset_fields = ['tenant_id', 'category_guid']

class ToastDiningOptionViewSet(viewsets.ModelViewSet):
    queryset = ToastDiningOption.objects.all()
    serializer_class = ToastDiningOptionSerializer
    filterset_fields = ['tenant_id', 'option_guid']

class ToastServiceAreaViewSet(viewsets.ModelViewSet):
    queryset = ToastServiceArea.objects.all()
    serializer_class = ToastServiceAreaSerializer
    filterset_fields = ['tenant_id', 'area_guid']

class ToastPaymentViewSet(viewsets.ModelViewSet):
    queryset = ToastPayment.objects.all()
    serializer_class = ToastPaymentSerializer
    filterset_fields = ['tenant_id', 'payment_guid', 'order_guid']
