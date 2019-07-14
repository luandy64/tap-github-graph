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
    resp = requests.post('https://api.github.com/graphql',
                         json={'query':query},
                         headers=headers)

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
        # if cursor is passed in, then add_cursor gets a value that it
        # contributes to stream_line. Otherwise we interpolate in nothing
        add_cursor = 'after: "%s"' % cursor

    stream_line = '%s(first:1 %s)' % (stream_name, add_cursor)

    return base_query % (CONFIG['repository'],
                         CONFIG['owner'],
                         stream_line,
                         catalog_entry)


def do_discover(config):
    LOGGER.info('Running discovery')

    return {"streams": "foo"}


def sync(stream_name, catalog_entry, state):
    query = format_query(stream_name, catalog_entry)
    response = make_graphql(query)

    response_obj = response['data']['repository'][stream_name]
    has_next_page = response_obj['pageInfo']['hasNextPage']

    # Grab the object off the response. The 0 index at the end is because we
    # only ever ask for one object at a time
    current_object = response_obj['edges'][0]

    while has_next_page:
        record = current_object['node']
        pagination_cursor = current_object['cursor']

        # TODO: Change this to singer messages
        print(json.dumps(record))

        query = format_query(stream_name, catalog_entry, pagination_cursor)
        response = make_graphql(query)

        response_obj = response['data']['repository'][stream_name]
        has_next_page = response_obj['pageInfo']['hasNextPage']
        current_object = response_obj['edges'][0]

    # if
    #   1. we only get one object back
    #   2. we are at the end of pagination, because the object has a
    #      `hasNextPage` == false,
    # we have to do this round of processing
    record = current_object['node']
    pagination_cursor = current_object['cursor']

    # TODO: Change this to singer messages
    print(json.dumps(record))


def do_sync(catalog, state):
    LOGGER.info('Running sync')

    STREAMS = [
        'assignableUsers',
        'collaborators',
        'issues',
    ]

    for stream in STREAMS:
        LOGGER.info('Getting all: %s', stream)
        catalog_entry = catalog['streams'][stream]
        sync(stream, catalog_entry, state)
        LOGGER.info('FINISHED: %s', stream)


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    if not args.catalog:
        args.catalog = do_discover(args.config)

    if args.discover:
        print(json.dumps(args.catalog, indent=2))
    else:
        CONFIG.update(args.config)

        # TODO: Actually implement discovery
        with open('/tmp/tgg_catalog') as bad_catalog:
            args.catalog = json.load(bad_catalog)

        do_sync(
            catalog=args.catalog,
            state=args.state
        )


if __name__ == '__main__':
    main()
