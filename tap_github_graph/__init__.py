#!/usr/bin/env python3

import json
import requests
import singer
import singer.utils as utils

CONFIG = {}

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'access_token',
    'repository',
    'owner'
]

def parse_catalog_entry(catalog_entry):
    # TODO: Write a function to take a catalog_entry and parse it to be the
    # selected fields separated by a space. There is a tricky bit where the
    # field is an object and we have to select a nested field. It's not tricky
    # if we don't expose the nested field at all and just document "If you
    # select W, then you get {X, Y, and Z}"

    # Should be equal to this in the end
    # catalog_entry = " ".join(['url',
    #                       'labels {totalCount}',
    #                       'repository {url}',
    #                       'number',
    #                       'closedAt',
    #                       'title',
    #                       'updatedAt',
    #                       'authorAssociation',
    #                       'locked'])
    pass

def make_graphql(query):
    headers = {"Authorization" : "Bearer " + CONFIG['access_token']}
    resp = requests.post('https://api.github.com/graphql', json={'query':query}, headers=headers)

    resp.raise_for_status()
    return resp.json()


def format_query(stream_name, catalog_entry, cursor=None):
    """
    GraphQL queries have the format:
        {
          repository(name:"repo" owner:"owner")
          {
            stream_name(first:1)
            {
              pageInfo {
                hasNextPage
              }
              edges {
                cursor
                node {
                  field1
                  field2
                  fieldn
                }
              }
            }
          }
        }
    """
    base_query = (
        '{'
        '  repository(name:"%s" owner:"%s")'
        '  {'
        '    %s' # replace with stream_line
        '    {'
        '      pageInfo {'
        '        hasNextPage'
        '      }'
        '      edges {'
        '        cursor'
        '        node {'
        '         %s'
        '        }'
        '      }'
        '    }'
        '  }'
        '}'
    )

    add_cursor = ''
    if cursor:
        # if cursor is passed in, then add_cursor gets a value that it contributes to stream_line
        # Otherwise we interpolate in nothing
        add_cursor = 'after: "%s"' % cursor

    stream_line = '%s(first:1 %s)' % (stream_name, add_cursor)

    return base_query % (CONFIG['repository'], CONFIG['owner'], stream_line, catalog_entry)


def get_all_issues(stream_name, catalog_entry, state):
    catalog_entry = (
        'url '
        'labels {'
        '  totalCount'
        '} '
        'repository {'
        '  url'
        '} '
        'number '
        'closedAt '
        'title '
        'updatedAt '
        'authorAssociation '
        'locked '
        )

    # TODO: Write a function to take a catalog_entry and parse it to be like this
    catalog_entry = " ".join(['url',
                              'labels {totalCount}', # This part is the tricky bit
                              'repository {url}',
                              'number',
                              'closedAt',
                              'title',
                              'updatedAt',
                              'authorAssociation',
                              'locked'])

    query = (
        '{' +
        '    repository(name:"{}" owner:"{}")'.format(repo, owner) +
        '  {' +
        '    {}(first:1)'.format(stream_name) +
        '    {'
        '      pageInfo {'
        '        hasNextPage'
        '      }'
        '      edges {'
        '        cursor'
        '        node {' +
        catalog_entry +
        '        }'
        '      }'
        '    }'
        '  } '
        '}')

    # Best way to do this?
    query = format_query(repo, owner, stream_name, catalog_entry)

    response = make_graphql(query)

    issues_obj = response['data']['repository']['issues']

    has_next_page = issues_obj['pageInfo']['hasNextPage']

    while has_next_page:
        #
        # process the response
        #

        # grab the object off the response
        # the 0 index at the end is because we only ever ask for one object at a time
        current_object = issues_obj['edges'][0]

        record = current_object['node']
        pagination_cursor = current_object['cursor']

        # Write record
        print(json.dumps(record))


        # format next query

        # {{ because that's how to you don't confuse python
        add_cursor = '    issues(first:1 after: "{}") {{'.format(pagination_cursor)

        query ="""
{
    repository(name:"tap-github" owner:"singer-io") {
""" + add_cursor + """
      pageInfo {
        hasNextPage
      }
      edges {
        cursor
        node {
          url
          labels {
            totalCount
          }
          repository {
            url
          }
          number
          closedAt
          title
          updatedAt
          authorAssociation
          locked
        }
      }
    }
  }
}
"""

        query = format_query(repo, owner, stream_name, catalog_entry, pagination_cursor)

        #
        # make the new request
        #
        response = make_graphql(query)

        issues_obj = response['data']['repository']['issues']
        has_next_page = issues_obj['pageInfo']['hasNextPage']

    # if we only get one object back, or we are at the end of pagination, because the object has a `hasNextPage` == false, we have to do one more processing round
    current_object = issues_obj['edges'][0]
    record = current_object['node']
    pagination_cursor = current_object['cursor']

    # Write record
    print(json.dumps(record))


def get_all_collaborators(stream_name, catalog_entry, state):

    query = """
{
  repository(name: "tap-github", owner: "singer-io") {
    collaborators(first:1) {
      pageInfo {
        hasNextPage
      }
      edges {
        cursor
        node {
          login
          id
          url
        }
      }
    }
  }
}
"""

    catalog_entry = " ".join(['login', 'id', 'url'])

    query = format_query(stream_name, catalog_entry)

    response = make_graphql(query)

    collaborators_obj = response['data']['repository']['collaborators']

    has_next_page = collaborators_obj['pageInfo']['hasNextPage']

    while has_next_page:
        #
        # process the response
        #

        # grab the object off the response
        # the 0 index at the end is because we only ever ask for one object at a time
        current_object = collaborators_obj['edges'][0]

        record = current_object['node']
        pagination_cursor = current_object['cursor']

        # Write record
        print(json.dumps(record))


        # format next query

        # {{ because that's how to you don't confuse python
        add_cursor = '    collaborators(first:1 after: "{}") {{'.format(pagination_cursor)

        query = """
{
  repository(name: "tap-github" owner: "singer-io") {
""" + add_cursor + """
      pageInfo {
        hasNextPage
      }
      edges {
        cursor
        node {
          name
          login
          id
        }
      }
    }
  }
}
"""

        query = format_query(stream_name, catalog_entry, pagination_cursor)

        #
        # make the new request
        #
        response = make_graphql(query)

        collaborators_obj = response['data']['repository']['collaborators']
        has_next_page = collaborators_obj['pageInfo']['hasNextPage']

    # if we only get one object back, or we are at the end of pagination, because the object has a `hasNextPage` == false, we have to do one more processing round
    current_object = collaborators_obj['edges'][0]
    record = current_object['node']
    pagination_cursor = current_object['cursor']

    # Write record
    print(json.dumps(record))



def get_all_assignable_users(stream_name, catalog_entry, state):

    query = """
{
  repository(name: "tap-github" owner: "singer-io") {
    assignableUsers(first:1) {
      pageInfo {
        hasNextPage
      }
      edges {
        cursor
        node {
          name
          login
          id
        }
      }
    }
  }
}
"""
    catalog_entry = " ".join(['name', 'login', 'id'])

    query = format_query(stream_name, catalog_entry)

    response = make_graphql(query)

    assignable_users_obj = response['data']['repository']['assignableUsers']

    has_next_page = assignable_users_obj['pageInfo']['hasNextPage']

    while has_next_page:
        #
        # process the response
        #

        # grab the object off the response
        # the 0 index at the end is because we only ever ask for one object at a time
        current_object = assignable_users_obj['edges'][0]

        record = current_object['node']
        pagination_cursor = current_object['cursor']

        # Write record
        print(json.dumps(record))


        # format next query

        # {{ because that's how to you don't confuse python
        add_cursor = '    assignableUsers(first:1 after: "{}") {{'.format(pagination_cursor)

        query = """
{
  repository(name: "tap-github" owner: "singer-io") {
""" + add_cursor + """
      pageInfo {
        hasNextPage
      }
      edges {
        cursor
        node {
          name
          login
          id
        }
      }
    }
  }
}
"""
        query = format_query(stream_name, catalog_entry, pagination_cursor)

        #
        # make the new request
        #
        response = make_graphql(query)

        assignable_users_obj = response['data']['repository']['assignableUsers']
        has_next_page = assignable_users_obj['pageInfo']['hasNextPage']

    # if we only get one object back, or we are at the end of pagination, because the object has a `hasNextPage` == false, we have to do one more processing round
    current_object = assignable_users_obj['edges'][0]
    record = current_object['node']
    pagination_cursor = current_object['cursor']

    # Write record
    print(json.dumps(record))


def do_discover(config):
    LOGGER.info('Running discovery')

    return {"streams": "foo"}

sync_stream = {
    'assignableUsers' : get_all_assignable_users,
    'collaborators' : get_all_collaborators,
    'issues' : get_all_issues,
}

def do_sync(catalog, state):
    LOGGER.info('Running sync')

    STREAMS = [
        'assignableUsers',
    #    'collaborators'
    #    'issues'
    ]

    for stream in STREAMS:
        LOGGER.info('Getting all: %s', stream)
        sync_stream[stream](stream, {}, state)
        LOGGER.info('FINISHED: %s', stream)


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if not args.catalog:
        catalog = do_discover(args.config)

    if args.discover:
        print(json.dumps(catalog, indent=2))
    else:
        CONFIG.update(args.config)

        do_sync(
            catalog=args.catalog,
            state=args.state
        )

if __name__ == '__main__':
    main()
