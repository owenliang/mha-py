# user-defined hooks

def before_ha(cluster):
    print('before_ha',cluster)

def after_ha(cluster,topology,exception=None):
    print('after_ha',cluster,topology,exception)