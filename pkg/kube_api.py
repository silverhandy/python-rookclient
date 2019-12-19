#   Copyright 2011 OpenStack Foundation
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#   Author: Tingjie Chen <tingje.chen@intel.com>
#   Credit: python-rookclient
#

"""
A rook-api python interface that handles rook yaml & REST calls and response.
"""
import subprocess
import sys
import yaml
#import tenacity
import string

class ApiError(Exception):
    pass

class KubeOperator(object):

    def __init__(self):
        """
        Initialize the class, get the necessary parameters
        """
        pass

    def build_kuebctl_command(self, basic_command, namespace=None,
                              resource=None, name=None, flags=None,
                              with_definition=False):
        command = ['kubectl', basic_command]
        if namespace:
            command += ['--namespace', namespace]
        if resource:
            command.append(resource)
        if name:
            command.append(name)
        if flags:
            command += flags
        if with_definition:
            command += ['-f', '-']
        return command

    def execute_kubectl_command(self, command, definition):
        execute_process = subprocess.Popen(command, stdin=subprocess.PIPE,
            stderr=subprocess.PIPE)
        stdout, stderr = execute_process.communicate(yaml.dump(definition).encode())
        execute_process.stdin.close()
        if execute_process.wait() != 0:
            raise ApiError(stderr)

    def execute_kubectl_command_with_return(self, command):
        execute_process = subprocess.Popen(command, stdout=subprocess.PIPE,
            stderr=sys.stderr)
        objects = yaml.safe_load(execute_process.stdout)
        if execute_process.wait() != 0:
            raise ApiError
        return objects

    #@tenacity.retry(reraise=True, 
    #                retry=tenacity.retry_if_exception_type(ApiError),
    #                stop=tenacity.stop_after_attempt(3))
    def command_get(self, resource, name=None, namespace=None):
        command = self.build_kuebctl_command('get', namespace=namespace,
            resource=resource, name=name, flags=['-o', 'yaml'])
        return self.execute_kubectl_command_with_return(command)

    def command_find_pod(self, app, id_key=None, id_value=None, namespace=None):
        if id_key and id_value:
            flags = ['-l', 'app=%s,%s=%s'%(app, id_key, id_value)]
        else:
            flags = ['-l', 'app=%s'%app]
        command = self.build_kuebctl_command('get', namespace=namespace,
            resource = 'pod',
            flags = flags + ['-o', r'jsonpath="{.items[0].metadata.name}"'])
        #print("Get pod command: %s" %command)
        return self.execute_kubectl_command_with_return(command)

    def command_replace(self, definition, namespace=None):
        command = self.build_kuebctl_command('replace', namespace=namespace,
            flags=['--cascade'], with_definition=True)
            #flags=['--force', '--cascade'], with_definition=True)
        self.execute_kubectl_command(command, definition)

    def command_execute_cli(self, pod, cli, namespace=None):
        command = self.build_kuebctl_command('exec', namespace=namespace,
            name=pod, flags=['-ti', '--', 'bash', '-c', '%s'%cli])
        #print("Exec cli command: %s" %command)
        return self.execute_kubectl_command_with_return(command)

    def command_delete(self, resource, name, namespace=None):
        command = self.build_kuebctl_command('delete', namespace=namespace,
            resource=resource, name=name)
            #flags=['--force', '--cascade'], with_definition=True)
        subprocess.check_call(command)

    def get_object_value(self, objects, key):
        path = key.split('.')
        data = objects
        for item in path:
            data = data[item]

        return data

    def set_object_value(self, objects, key, value):
        path = key.split('.')[0:-1]
        final_key = key.split('.')[-1]
        data = objects
        for item in path:
            data = data[item]
        if data[final_key] != value:
            data[final_key] = value

        return objects

    def override_resource(self, namespace, resource, name, key, value):
        """
        Implement the function to override pararmeters in ceph-cluster helm
        chart.
        """
        objects = self.command_get(resource, name, namespace)
        if not objects:
            print("Fail to get resource %s." %name)
            return
        
        if self.get_object_value(objects, key) is None:
            print("Fail to get resource object %s." %key)
            return
        
        new_objects = self.set_object_value(objects, key, value)
        #print("New Object before replace ............\n")
        #print(new_objects)

        self.command_replace(new_objects, namespace)
