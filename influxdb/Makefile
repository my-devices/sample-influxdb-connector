#
# Makefile
#
# Makefile for macchina.io Remote Manager InfluxDB integration
#

BUNDLE_TOOL = $(POCO_BASE)/OSP/BundleCreator/$(BINDIR)/bundle

include $(POCO_BASE)/build/rules/global

INCLUDE += -I../core/include

objects = \
	BundleActivator

target         = net.my-devices.influxdb
target_version = 1
target_libs    = net.my-devices.core PocoOSP PocoZip PocoNet PocoUtil PocoXML PocoJSON PocoFoundation

postbuild      = $(BUNDLE_TOOL) -n$(OSNAME) -a$(OSARCH) -o../bundles influxdb.bndlspec

include $(POCO_BASE)/build/rules/dylib
