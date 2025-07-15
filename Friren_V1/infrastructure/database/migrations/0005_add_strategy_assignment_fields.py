# Generated migration for strategy assignment system
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database', '0004_create_watchlist_table'),
    ]

    operations = [
        migrations.AddField(
            model_name='currentholdings',
            name='current_strategy',
            field=models.CharField(blank=True, help_text='Currently assigned trading strategy', max_length=50, null=True),
        ),
        migrations.AddField(
            model_name='currentholdings',
            name='assignment_scenario',
            field=models.CharField(
                blank=True,
                choices=[
                    ('user_buy_hold', 'User Buy & Hold'),
                    ('decision_engine_choice', 'Decision Engine Algorithmic'),
                    ('strategy_reevaluation', 'Position Health Monitor Reassignment')
                ],
                help_text='How this strategy was assigned',
                max_length=30,
                null=True
            ),
        ),
        migrations.AddField(
            model_name='currentholdings',
            name='assignment_reason',
            field=models.CharField(
                blank=True,
                choices=[
                    ('new_position', 'New Position'),
                    ('regime_change', 'Market Regime Change'),
                    ('poor_performance', 'Poor Performance'),
                    ('diversification', 'Portfolio Diversification'),
                    ('manual_override', 'Manual Override'),
                    ('rebalance', 'Periodic Rebalance'),
                    ('user_buy_hold', 'User Buy & Hold'),
                    ('decision_engine_choice', 'Decision Engine Choice'),
                    ('strategy_reevaluation', 'Strategy Reevaluation')
                ],
                help_text='Reason for current strategy assignment',
                max_length=30,
                null=True
            ),
        ),
        migrations.AddField(
            model_name='currentholdings',
            name='strategy_confidence',
            field=models.DecimalField(
                blank=True,
                decimal_places=2,
                help_text='Confidence score for current strategy assignment (0-100)',
                max_digits=5,
                null=True
            ),
        ),
        migrations.AddField(
            model_name='currentholdings',
            name='strategy_assigned_at',
            field=models.DateTimeField(
                blank=True,
                help_text='When current strategy was assigned',
                null=True
            ),
        ),
        migrations.AddField(
            model_name='currentholdings',
            name='previous_strategy',
            field=models.CharField(
                blank=True,
                help_text='Previous strategy before reassignment',
                max_length=50,
                null=True
            ),
        ),
        migrations.AddField(
            model_name='currentholdings',
            name='assignment_metadata',
            field=models.JSONField(
                blank=True,
                help_text='Additional metadata about strategy assignment',
                null=True
            ),
        ),
        migrations.AddIndex(
            model_name='currentholdings',
            index=models.Index(fields=['current_strategy'], name='database_cur_current_4b72d4_idx'),
        ),
        migrations.AddIndex(
            model_name='currentholdings',
            index=models.Index(fields=['assignment_scenario'], name='database_cur_assignm_5a8e7c_idx'),
        ),
        migrations.AddIndex(
            model_name='currentholdings',
            index=models.Index(fields=['strategy_assigned_at'], name='database_cur_strateg_9f3b2a_idx'),
        ),
    ]