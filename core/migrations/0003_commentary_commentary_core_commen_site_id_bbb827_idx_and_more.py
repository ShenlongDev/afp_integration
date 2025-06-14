# Generated by Django 4.2 on 2025-04-23 17:33

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0002_integrationsitemapping_site_address_line1_and_more'),
    ]

    operations = [
        migrations.CreateModel(
            name='Commentary',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('comments', models.TextField(help_text='The commentary text')),
                ('takings', models.DecimalField(blank=True, decimal_places=2, help_text='Associated takings amount', max_digits=10, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('site', models.ForeignKey(help_text='The site this commentary is associated with', on_delete=django.db.models.deletion.CASCADE, related_name='commentaries', to='core.site')),
                ('user', models.ForeignKey(help_text='The user who created this commentary', on_delete=django.db.models.deletion.CASCADE, related_name='commentaries', to='core.user')),
            ],
            options={
                'verbose_name_plural': 'Commentaries',
            },
        ),
        migrations.AddIndex(
            model_name='commentary',
            index=models.Index(fields=['site'], name='core_commen_site_id_bbb827_idx'),
        ),
        migrations.AddIndex(
            model_name='commentary',
            index=models.Index(fields=['user'], name='core_commen_user_id_e7cd61_idx'),
        ),
        migrations.AddIndex(
            model_name='commentary',
            index=models.Index(fields=['created_at'], name='core_commen_created_080512_idx'),
        ),
    ]
