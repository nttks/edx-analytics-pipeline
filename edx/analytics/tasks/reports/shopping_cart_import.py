
"""Import Shopping Cart Tables from the LMS."""

import luigi
import luigi.hdfs

from edx.analytics.tasks.util.overwrite import OverwriteOutputMixin
from edx.analytics.tasks.database_imports import DatabaseImportMixin, \
    ImportShoppingCartCertificateItem, \
    ImportShoppingCartCourseRegistrationCodeItem, \
    ImportShoppingCartDonation, \
    ImportShoppingCartOrder, \
    ImportShoppingCartOrderItem, \
    ImportShoppingCartPaidCourseRegistration, \
    ImportProductCatalog, \
    ImportProductCatalogAttributes, \
    ImportProductCatalogAttributeValues, \
    ImportOrderOrderHistory, \
    ImportOrderHistoricalLine, \
    ImportCurrentBasketState, \
    ImportCurrentOrderState, \
    ImportCurrentOrderLineState, \
    ImportCurrentOrderLineAttributeState, \
    ImportCurrentOrderLinePriceState, \
    ImportOrderPaymentEvent, \
    ImportPaymentSource, \
    ImportPaymentTransactions, \
    ImportPaymentProcessorResponse


class PullFromShoppingCartTablesTask(DatabaseImportMixin, OverwriteOutputMixin, luigi.WrapperTask):
    """Imports a set of shopping cart database tables from an external LMS RDBMS into a destination directory."""

    def requires(self):
        kwargs = {
            'destination': self.destination,
            'credentials': self.credentials,
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            'overwrite': self.overwrite,
        }
        yield (
            # Original shopping cart tables
            ImportShoppingCartOrder(**kwargs),
            ImportShoppingCartOrderItem(**kwargs),
            ImportShoppingCartCertificateItem(**kwargs),
            ImportShoppingCartPaidCourseRegistration(**kwargs),
            ImportShoppingCartDonation(**kwargs),
            ImportShoppingCartCourseRegistrationCodeItem(**kwargs),
        )

    def output(self):
        return [task.output() for task in self.requires()]


class PullFromEcommerceTablesTask(DatabaseImportMixin, OverwriteOutputMixin, luigi.WrapperTask):
    """Imports a set of ecommerce tables from an external database into a destination directory."""

    def requires(self):
        kwargs = {
            'destination': self.destination,
            'credentials': self.credentials,
            'num_mappers': self.num_mappers,
            'verbose': self.verbose,
            'import_date': self.import_date,
            'overwrite': self.overwrite,
        }
        yield (
            # Oscar Product Tables
            ImportProductCatalog(**kwargs),
            ImportProductCatalogAttributes(**kwargs),
            ImportProductCatalogAttributeValues(**kwargs),

            # Oscar Order History Tables
            ImportOrderOrderHistory(**kwargs),
            ImportOrderHistoricalLine(**kwargs),

            # Oscar Current State and Line Item Tables
            ImportCurrentBasketState(**kwargs),
            ImportCurrentOrderState(**kwargs),
            ImportCurrentOrderLineState(**kwargs),
            ImportCurrentOrderLineAttributeState(**kwargs),
            ImportCurrentOrderLinePriceState(**kwargs),

            # Oscar Payment Tables
            ImportOrderPaymentEvent(**kwargs),
            ImportPaymentSource(**kwargs),
            ImportPaymentTransactions(**kwargs),
            ImportPaymentProcessorResponse(**kwargs),
        )

    def output(self):
        return [task.output() for task in self.requires()]