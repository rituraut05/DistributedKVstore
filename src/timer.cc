#include "timer.hh"

util::Timer::Timer()
{
	
}

util::Timer::Timer(int const& tick_duration, int timeout)
{
	set_tick_duration(tick_duration);
	reset(timeout);
}

/* Protected */

void util::Timer::calculate()
{
	_end = std::chrono::system_clock::now();
	_elapsed = _end - _start;
	int e = (int) (_elapsed.count() * 1000);
	if(e >= _tick_duration && e > 0)
	{
		_update = true;
		int t = (e - (e % _tick_duration))/_tick_duration;
		_current_tick += t;
		_start = _end;
	}
	else
	{
		_update = false;
	}
}

/* Getters */

int util::Timer::get_tick() // Returns current tick
{
	calculate();
	return _current_tick;
}

/* Setters */

void util::Timer::set_tick_duration(int const& d)
{
	_tick_duration = d;
}

void util::Timer::set_running(bool running)
{
  _running = running;
}

/* Methods */

void util::Timer::start(int timeout)
{
	reset(timeout);
}

void util::Timer::reset(int timeout)
{
	_update = true;
	_current_tick = 0;
  _timeout = timeout;
	_start = std::chrono::system_clock::now();
}

bool util::Timer::updated()
{
	return _update;
}

bool util::Timer::running()
{
  return _running;
}