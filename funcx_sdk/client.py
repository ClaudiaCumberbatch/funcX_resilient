from funcx_sdk.utils.auth import do_login_flow, make_authorizer, logout
from funcx_sdk.config import check_logged_in, FUNCX_SERVICE_ADDRESS
from globus_sdk.base import BaseClient, slash_join
from tempfile import mkstemp
import pickle as pkl
import pandas as pd
import requests
import codecs
import json
import os

# jsonpickle_numpy.register_handlers()


class funcXClient(BaseClient):
    """Main class for interacting with the funcX service

    Holds helper operations for performing common tasks with the funcX service.
    """

    def __init__(self, authorizer, http_timeout=None, **kwargs):
        """Initialize the client

        Args:
            authorizer (:class:`GlobusAuthorizer <globus_sdk.authorizers.base.GlobusAuthorizer>`):
                An authorizer instance used to communicate with funcX
            http_timeout (int): Timeout for any call to service in seconds. (default is no timeout)
        Keyword arguments are the same as for BaseClient
        """
        super(funcXClient, self).__init__("funcX", environment='funcx', authorizer=authorizer,
                                          http_timeout=http_timeout, base_url=FUNCX_SERVICE_ADDRESS,
                                          **kwargs)

    @classmethod
    def login(cls, force=False, **kwargs):
        """Create a funcXlient with credentials

        Either uses the credentials already saved on the system or, if no credentials are present
        or ``force=True``, runs a login procedure to get new credentials

        Keyword arguments are passed to the funcXClient constructor

        Args:
            force (bool): Whether to force a login to get new credentials
        Returns:
            (funcXClient) A client complete with proper credentials
        """

        # If not logged in or `force`, get credentials
        if force or not check_logged_in():
            # Revoke existing credentials
            if check_logged_in():
                logout()

            # Ask for user credentials, save the resulting Auth tokens to disk
            do_login_flow()

        # Makes an authorizer
        rf_authorizer = make_authorizer()

        return funcXClient(rf_authorizer, **kwargs)

    def get_task_status(self, task_id):
        """Get the status of a funcX task.

        Args:
            task_id (string): UUID of the task
        Returns:
            (dict) status block containing "status" key.
        """

        r = self.get("{task_id}/status".format(task_id=task_id))
        print (r)
        return r.text
        # return r.json()

    def run(self, inputs, input_type='json'):
        """Initiate an invocation

        Args:
            inputs: Data to be used as input to the function. Can be a string of file paths or URLs
            input_type (string): How to send the data to funcX. Can be "python" (which pickles
                the data), "json" (which uses JSON to serialize the data), or "files" (which
                sends the data as files).
        Returns:
            Reply from the service
        """
        servable_path = 'execute'

        # Prepare the data to be sent to funcX
        if input_type == 'python':
            data = {'python': codecs.encode(pkl.dumps(inputs), 'base64').decode()}
        elif input_type == 'json':
            data = {'data': inputs}
        elif input_type == 'files':
            raise NotImplementedError('Files support is not yet implemented')
        else:
            raise ValueError('Input type not recognized: {}'.format(input_type))

        # Send the data to funcX
        r = self.post(servable_path, json_body=data)
        if r.http_status is not 200:
            raise Exception(r)

        # Return the result
        return r.data


    def register_endpoint(self, name, description=None):
        """Register an endpoint with the funcX service.

        Args:
            name: str name of the endpoint
            description: str describing the site
        Returns:
            The port to connect to
        """
        registration_path = 'register_site'

        data = {"sitename": sitename, "description": description}

        r = self.post(registration_path, json_body=data)
        if r.http_status is not 200:
            raise Exception(r)

        # Return the result
        return r.data['port']


