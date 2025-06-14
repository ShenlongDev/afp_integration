from django.db import models
from django.utils import timezone
from django.contrib.auth.hashers import make_password, check_password
from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError


class Client(models.Model):
    STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('suspended', 'Suspended'),
    ]
    
    BILLING_CYCLE_CHOICES = [
        ('monthly', 'Monthly'),
        ('quarterly', 'Quarterly'),
        ('annually', 'Annually'),
    ]
    
    name = models.CharField(max_length=255)
    industry = models.CharField(max_length=255)
    timezone = models.CharField(max_length=50)
    primary_contact = models.CharField(max_length=255)
    reporting_calendar = models.JSONField()
    subscription_plan = models.CharField(max_length=255)
    billing_email = models.EmailField()
    billing_cycle = models.CharField(max_length=20, choices=BILLING_CYCLE_CHOICES)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='active')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name


class Organisation(models.Model):
    STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
    ]
    
    name = models.CharField(max_length=255)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='active')
    client = models.ForeignKey(Client, on_delete=models.CASCADE, related_name='organisations')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name


class Site(models.Model):
    """
    A unified model for managing sites/locations across different integrations.
    Contains general site information that applies to all integrations.
    """
    STATUS_CHOICES = [
        ('active', 'Active'),
        ('inactive', 'Inactive'),
        ('closed', 'Closed'),
    ]
    
    organisation = models.ForeignKey(Organisation, on_delete=models.CASCADE, related_name="sites")
    name = models.CharField(max_length=255, db_index=True)
    code = models.CharField(max_length=50, null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    
    # Location details
    postcode = models.CharField(max_length=20)
    region = models.CharField(max_length=255)
    address_line1 = models.CharField(max_length=255, null=True, blank=True)
    address_line2 = models.CharField(max_length=255, null=True, blank=True)
    city = models.CharField(max_length=100, null=True, blank=True)
    state_code = models.CharField(max_length=50, null=True, blank=True)
    zip_code = models.CharField(max_length=20, null=True, blank=True)
    country = models.CharField(max_length=100, null=True, blank=True)
    phone = models.CharField(max_length=20, null=True, blank=True)
    
    # Business details
    opened_date = models.DateField()
    timezone = models.CharField(max_length=50, null=True, blank=True)
    currency_code = models.CharField(max_length=10, null=True, blank=True)
    
    # Status
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='active', db_index=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} ({self.organisation.name})"

    class Meta:
        indexes = [
            models.Index(fields=["organisation"]),
            models.Index(fields=["name"]),
            models.Index(fields=["status"]),
        ]
        unique_together = ('organisation', 'name')


class TaskLog(models.Model):
    task_name = models.CharField(max_length=255)
    status = models.CharField(max_length=50)
    detail = models.TextField()
    timestamp = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return f"{self.task_name} - {self.status}"


class User(models.Model):
    """
    Custom user model without any Django auth inheritance.
    """
    username = models.CharField(max_length=150, unique=True)
    email = models.EmailField(unique=True)
    password = models.CharField(max_length=128)
    first_name = models.CharField(max_length=30, blank=True)
    last_name = models.CharField(max_length=150, blank=True)
    is_active = models.BooleanField(default=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):

        return self.email

    def set_password(self, raw_password):
        self.password = make_password(raw_password)

    def check_password(self, raw_password):
        return check_password(raw_password, self.password)

    def has_access_to_client(self, client):
        """Check if user has direct access to a client"""
        return self.user_access.filter(
            object_type='client',
            object_id=client.id
        ).exists()

    def has_access_to_organisation(self, organisation):
        """Check if user has access to an organisation (directly or through client)"""
        return (
            self.user_access.filter(
                object_type='organisation',
                object_id=organisation.id
            ).exists() or
            self.has_access_to_client(organisation.client)
        )

    def has_access_to_site(self, site):
        """Check if user has access to a site (directly or through organisation/client)"""
        return (
            self.user_access.filter(
                object_type='site',
                object_id=site.id
            ).exists() or
            self.has_access_to_organisation(site.organisation)
        )

    def get_accessible_clients(self):
        """Get all clients the user has access to"""
        return Client.objects.filter(
            id__in=self.user_access.filter(object_type='client').values('object_id')
        )

    def get_accessible_organisations(self):
        """Get all organisations the user has access to (directly or through client)"""
        return Organisation.objects.filter(
            models.Q(id__in=self.user_access.filter(object_type='organisation').values('object_id')) |
            models.Q(client__in=self.get_accessible_clients())
        )

    def get_accessible_sites(self):
        """Get all sites the user has access to (directly or through organisation/client)"""
        return Site.objects.filter(
            models.Q(id__in=self.user_access.filter(object_type='site').values('object_id')) |
            models.Q(organisation__in=self.get_accessible_organisations())
        )

    def has_role(self, role, content_object):
        """
        Check if user has a specific role for a given content object (Client, Organisation, or Site)
        """
        return self.user_access.filter(
            object_type=content_object.__class__.__name__.lower(),
            object_id=content_object.id,
            role=role
        ).exists()

    def get_sites_with_role(self, role):
        """Get all sites where the user has a specific role"""
        return Site.objects.filter(
            id__in=self.user_access.filter(
                object_type='site',
                role=role
            ).values('object_id')
        )


class UserAccess(models.Model):
    """
    Model to manage user access permissions across the hierarchy.
    A user can have access to any model in the hierarchy (Client, Organisation, or Site).
    """
    ROLE_CHOICES = [
        ('viewer', 'Viewer'),
        ('editor', 'Editor'),
        ('manager', 'Manager'),
        ('admin', 'Administrator'),
    ]

    OBJECT_TYPE_CHOICES = [
        ('client', 'Client'),
        ('organisation', 'Organisation'),
        ('site', 'Site'),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='user_access')
    
    # Object type and ID fields
    object_type = models.CharField(
        max_length=20,
        choices=OBJECT_TYPE_CHOICES,
        help_text='The type of object this access is for'
    )
    object_id = models.PositiveIntegerField()
    
    # Role field to specify the level of access
    role = models.CharField(
        max_length=20,
        choices=ROLE_CHOICES,
        default='viewer',
        help_text='The role/level of access the user has for this object'
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ('user', 'object_type', 'object_id')
        indexes = [
            models.Index(fields=['object_type', 'object_id']),
            models.Index(fields=['role']),
        ]

    def __str__(self):
        return f"{self.user.email} -> {self.object_type}: {self.object_id} ({self.get_role_display()})"

    def clean(self):
        """Ensure the object type is valid"""
        if self.object_type not in dict(self.OBJECT_TYPE_CHOICES):
            raise ValidationError(
                f"Object type must be one of: {', '.join(t[0] for t in self.OBJECT_TYPE_CHOICES)}"
            )


class IntegrationSiteMapping(models.Model):
    """
    Maps sites to their integration-specific identifiers and settings.
    This allows a single site to be connected to multiple integrations.
    """
    site = models.ForeignKey(Site, on_delete=models.CASCADE, related_name="integration_mappings")
    integration = models.ForeignKey('integrations.Integration', on_delete=models.CASCADE)
    
    # Integration-specific identifiers
    external_id = models.CharField(max_length=255, db_index=True)
    external_name = models.CharField(max_length=255, null=True, blank=True)
    
    # Integration-specific settings
    settings = models.JSONField(default=dict, blank=True)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.site.name} - {self.integration.name} ({self.external_id})"

    class Meta:
        unique_together = ('site', 'integration', 'external_id')
        indexes = [
            models.Index(fields=["site", "integration"]),
            models.Index(fields=["external_id"]),
        ]


class Commentary(models.Model):
    """
    Model for storing comments and takings associated with sites and users.
    """
    comments = models.TextField(help_text="The commentary text")
    takings = models.DecimalField(
        max_digits=10,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Associated takings amount"
    )
    site = models.ForeignKey(
        Site,
        on_delete=models.CASCADE,
        related_name="commentaries",
        help_text="The site this commentary is associated with"
    )
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="commentaries",
        help_text="The user who created this commentary"
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Commentary by {self.user.email} for {self.site.name}"

    class Meta:
        indexes = [
            models.Index(fields=["site"]),
            models.Index(fields=["user"]),
            models.Index(fields=["created_at"]),
        ]
        verbose_name_plural = "Commentaries"


class Review(models.Model):
    """
    Model for storing customer reviews across different platforms and sources.
    """
    # Basic review information
    review_id = models.CharField(max_length=255, unique=True, db_index=True)
    review_text = models.TextField()
    review_date = models.DateTimeField()
    rating = models.DecimalField(max_digits=3, decimal_places=1)
    review_url = models.URLField(max_length=2048, null=True, blank=True)
    reviewer = models.CharField(max_length=255)
    
    # Location information
    client_name = models.CharField(max_length=255)
    store_name = models.CharField(max_length=255)
    city = models.CharField(max_length=255)
    place_id = models.CharField(max_length=255, null=True, blank=True)
    latitude = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    longitude = models.DecimalField(max_digits=9, decimal_places=6, null=True, blank=True)
    
    # Review parameters (using JSONField for flexibility)
    parameters = models.JSONField(
        default=dict,
        help_text="Stores review parameters and their ratings in a flexible format"
    )
    
    # Additional metadata
    recommend_score = models.DecimalField(max_digits=3, decimal_places=1, null=True, blank=True)
    visit_type = models.CharField(max_length=255, null=True, blank=True)
    source_system = models.CharField(max_length=255)
    automation = models.CharField(max_length=255, null=True, blank=True)
    
    # Relationships
    site = models.ForeignKey(
        Site,
        on_delete=models.CASCADE,
        related_name="reviews",
        null=True,
        blank=True
    )
    
    # Timestamps
    record_date = models.DateTimeField(auto_now_add=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["review_id"]),
            models.Index(fields=["review_date"]),
            models.Index(fields=["client_name"]),
            models.Index(fields=["store_name"]),
            models.Index(fields=["city"]),
            models.Index(fields=["site"]),
        ]
        ordering = ["-review_date"]

    def __str__(self):
        return f"Review by {self.reviewer} for {self.store_name} ({self.review_date})"


class Bulk_Calendar(models.Model):
    """
    Model for storing calendar data with organization-specific information.
    """
    client = models.ForeignKey(Client, on_delete=models.CASCADE, related_name='bulk_calendars')
    start_month = models.CharField(max_length=50)
    calendar_type = models.CharField(max_length=50)
    fiscal_year = models.CharField(max_length=50)
    date = models.DateField()
    month = models.CharField(max_length=50)
    month_sort = models.IntegerField()
    week_number = models.IntegerField()

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.client.name} - {self.date} - {self.calendar_type}"

    class Meta:
        indexes = [
            models.Index(fields=["client"]),
            models.Index(fields=["date"]),
            models.Index(fields=["fiscal_year"]),
        ]
        ordering = ["date"]
    