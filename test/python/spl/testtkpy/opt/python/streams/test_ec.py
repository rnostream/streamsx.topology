# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

# Import the SPL decorators
from streamsx.spl import spl
import streamsx.ec as ec

#------------------------------------------------------------------
# Test Execution Context (streamsx.ex) functions
#------------------------------------------------------------------

def splNamespace():
    return "com.ibm.streamsx.topology.pytest.pyec"

@spl.filter()
class PyTestOperatorContext:
    def __init__(self, job_id, pe_id, channel, local_channel, max_channels, local_max_channels):
        self.last = None
        self.job_id = job_id
        self.pe_id = pe_id
        self.channel = channel
        self.local_channel = local_channel
        self.max_channels = max_channels
        self.local_max_channels = local_max_channels
        if not self.check():
             raise AssertionError("PyTestOperatorContext")

    def same(self, expect, got):
        if expect != got:
            print("Expected", expect, "Got", got, flush=True)
            return False
        return True

    def check(self):
        ok = ec._supported
        ok = ok and self.same(self.job_id, ec.job_id())
        ok = ok and self.same(self.pe_id, ec.pe_id())
        ok = ok and self.same(self.channel, ec.channel(self))
        ok = ok and self.same(self.local_channel, ec.local_channel(self))
        ok = ok and self.same(self.max_channels, ec.max_channels(self))
        ok = ok and self.same(self.local_max_channels, ec.local_max_channels(self))
        return ok
            
    def __call__(self, *tuple):
        return self.check()


@spl.filter()
class PyTestMetrics:
    def __init__(self):
        ok = True
        self.c = ec.CustomMetric(self, "C1")
        ok = ok and self.check_metric(self.c, "C1", None, ec.MetricKind.Counter, 0)
        c2 = ec.CustomMetric(self, "C2", "This is C2")
        ok = ok and self.check_metric(c2, "C2", "This is C2", ec.MetricKind.Counter, 0)

        c3 = ec.CustomMetric(self, "C3", initialValue=8123)
        ok = ok and self.check_metric(c3, "C3", None, ec.MetricKind.Counter, 8123)

        g1 = ec.CustomMetric(self, "G1", kind=ec.MetricKind.Gauge)
        ok = ok and self.check_metric(g1, "G1", None, ec.MetricKind.Gauge, 0)

        g2 = ec.CustomMetric(self, "G2", kind='Gauge', initialValue=-214)
        ok = ok and self.check_metric(g2, "G2", None, ec.MetricKind.Gauge, -214)

        if not ok:
            raise AssertionError("Failed metrics!")

    def __call__(self, *tuple):
        ok= True
        cv = self.c.value

        self.c += 7
        ok = ok and self.check_metric(self.c, "C1", None, ec.MetricKind.Counter, cv + 7)

        self.c.value += 13
        ok = ok and self.check_metric(self.c, "C1", None, ec.MetricKind.Counter, cv + 7 + 13)
        return ok


    def check_metric(self, m, n, desc, k, v):
        if n != m.name:
            print(n, "!=", m.name)
            return False
        if desc is not None:
            if desc != m.description:
                print(desc, "!=", m.description)
                return False
        if k != m.kind:
            print(k, "!=", m.kind)
            return False

        if v != m.value:
            print("m.value", v, "!=", m.value)
            return False
        if v != int(m):
            print("int(m)", v, "!=", int(m))
            return False

        return True
