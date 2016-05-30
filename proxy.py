#!/usr/bin/env python3

import asyncio
import logging
import sys

BUFFER_SIZE = 65536

def create_logger():
  logger = logging.getLogger('proxy')
  logger.setLevel(logging.INFO)

  consoleHandler = logging.StreamHandler()
  consoleHandler.setLevel(logging.DEBUG)

  formatter = logging.Formatter(
    '%(asctime)s - %(threadName)s - %(message)s')
  consoleHandler.setFormatter(formatter)

  logger.addHandler(consoleHandler)

  return logger

logger = create_logger()

def client_connection_string(writer):
  return '{} -> {}'.format(
    writer.get_extra_info('peername'),
    writer.get_extra_info('sockname'))

def remote_connection_string(writer):
  return '{} -> {}'.format(
    writer.get_extra_info('sockname'),
    writer.get_extra_info('peername'))

async def proxy_data(reader, writer, connection_string):
  try:
    while True:
      buffer = await reader.read(BUFFER_SIZE)
      if not buffer:
        break
      writer.write(buffer)
      await writer.drain()
  except Exception as e:
    logger.info('proxy_data_task exception {}'.format(e))
  finally:
    writer.close()
    logger.info('close connection {}'.format(connection_string))

async def accept_client(client_reader, client_writer, remote_address, remote_port):
  client_string = client_connection_string(client_writer)
  logger.info('accept connection {}'.format(client_string))
  try:
    (remote_reader, remote_writer) = await asyncio.wait_for(
      asyncio.open_connection(host = remote_address, port = remote_port),
      timeout = 1)
  except asyncio.TimeoutError:
    logger.info('connect timeout')
    logger.info('close connection {}'.format(client_string))
    client_writer.close()
  except Exception as e:
    logger.info('error connecting to remote server: {}'.format(e))
    logger.info('close connection {}'.format(client_string))
    client_writer.close()
  else:
    remote_string = remote_connection_string(remote_writer)
    logger.info('connected to remote {}'.format(remote_string))
    asyncio.ensure_future(proxy_data(client_reader, remote_writer, remote_string))
    asyncio.ensure_future(proxy_data(remote_reader, client_writer, client_string))

def parse_addr_port_string(addr_port_string):
  addr_port_list = addr_port_string.rsplit(':', 1)
  return (addr_port_list[0], int(addr_port_list[1]))

def print_usage_and_exit():
  logger.error(
    'Usage: {} <listen addr> [<listen addr> ...] <remote addr>'.format(
      sys.argv[0]))
  sys.exit(1)

def main():
  if (len(sys.argv) < 3):
    print_usage_and_exit()

  try:
    local_address_port_list = map(parse_addr_port_string, sys.argv[1:-1])
    (remote_address, remote_port) = parse_addr_port_string(sys.argv[-1])
  except:
    print_usage_and_exit()

  def handle_client(client_reader, client_writer):
    asyncio.ensure_future(accept_client(
      client_reader = client_reader, client_writer = client_writer,
      remote_address = remote_address, remote_port = remote_port))

  loop = asyncio.get_event_loop()
  for (local_address, local_port) in local_address_port_list:
    try:
      server = loop.run_until_complete(
        asyncio.start_server(
          handle_client, host = local_address, port = local_port))
    except Exception as e:
      logger.error('Bind error: {}'.format(e))
      sys.exit(1)

    for s in server.sockets:
      logger.info('listening on {}'.format(s.getsockname()))

  try:
    loop.run_forever()
  except KeyboardInterrupt:
    pass

if __name__ == '__main__':
  main()
