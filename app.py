from waitress import serve
from pyramid.config import Configurator
from metadatachecker import *

def checkMetadata(request):
    uid = request.params.get('uid',None)
    if uid is None: return 'No UID provided'
    try:
        asdm = AsdmCheck()
        asdm.setUID(uid)
        if asdm.doCheck():
            return str(asdm.check)
        else:
            return str({'Message':'Check not completed'})
    except:
        print 'There was an error during Metadata Checker execution'


if __name__ == '__main__':
    config = Configurator()
    config.add_route('metadatachecker', '/')
    config.add_view(checkMetadata, route_name='metadatachecker',renderer='string')
    app = config.make_wsgi_app()
    serve(app, host='0.0.0.0', port=80)